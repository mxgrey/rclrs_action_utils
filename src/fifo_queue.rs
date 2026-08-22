use crate::FifoActionQueue;

use tokio::sync::oneshot::{self, Sender, Receiver};
use std::{
    sync::Arc,
    collections::VecDeque,
};
use rclrs::ActionIDL;
use futures::lock::Mutex;

/// A First-In-First-Out queue for regulating async behaviors. This is the
/// backbone of [`FifoActionQueue`].
#[derive(Clone)]
pub struct FifoQueue {
    pub(crate) internal: Arc<Mutex<FifoQueueInternal>>,
}

impl FifoQueue {
    /// Create a new queue. This queue can regulate any number of action servers.
    /// Using one queue for multiple action servers will ensure that only a single
    /// server can execute a single action at a time.
    pub fn new() -> Self {
        Self {
            internal: Default::default(),
        }
    }

    /// Specify an action type to queue.
    pub fn for_action<Action: ActionIDL>(&self) -> FifoActionQueue<Action> {
        FifoActionQueue::using_queue(self)
    }
}

#[derive(Default)]
pub(crate) struct FifoQueueInternal {
    queue: VecDeque<Sender<()>>,
    active: bool,
}

impl FifoQueueInternal {
    pub(crate) fn add_to_queue(&mut self) -> Option<Receiver<()>> {
        if self.active {
            let (sender, receiver) = oneshot::channel();
            self.queue.push_back(sender);
            return Some(receiver);
        }

        // An action for this queue is not active yet, so begin executing.
        self.active = true;
        return None;
    }

    pub(crate) fn next(&mut self) {
        if let Some(next) = self.queue.pop_front() {
            let _ = next.send(());
        } else {
            self.active = false;
        }
    }
}
