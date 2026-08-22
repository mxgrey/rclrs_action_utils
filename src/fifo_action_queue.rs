use crate::{FifoQueue, FifoQueueInternal};

use std::sync::Arc;
use futures::{
    future::BoxFuture,
    lock::Mutex,
};
use rclrs::{ActionIDL, AcceptedGoal, RequestedGoal, TerminatedGoal};

pub struct FifoActionQueue<Action> {
    internal: Arc<Mutex<FifoQueueInternal>>,
    _ignore: std::marker::PhantomData<fn(Action)>,
}

impl<Action: ActionIDL> FifoActionQueue<Action> {
    /// Create a new FIFO queue for the goal requests of an action server.
    pub fn new() -> Self {
        FifoQueue::new().for_action()
    }

    /// Process incoming action goal requests inline with an existing queue.
    /// This allows one FIFO queue to synchronize multiple different action types
    /// at the same time.
    pub fn using_queue(queue: &FifoQueue) -> Self {
        Self {
            internal: Arc::clone(&queue.internal),
            _ignore: Default::default(),
        }
    }

    /// Implement an action server using this FIFO queue.
    ///
    /// As soon as a goal is received from an action client, the `accept` callback
    /// will be run to immediately decide if the goal should be accepted. This is
    /// run immediately even if another goal is being executed, that way the client
    /// does not need to wait long to find out if the goal will be rejected.
    ///
    /// If the goal is accepted, then the action server will wait until all other
    /// actions are finished executing, and then begin executing the goal. That's
    /// when the `execute` callback will be triggered.
    ///
    /// # Examples
    ///
    /// ```
    /// # use rclrs::*;
    /// # use rclrs_action_utils::*;
    /// # use std::{
    /// #   sync::Arc,
    /// #   time::Duration,
    /// # };
    /// #
    /// # let context = Context::default();
    /// # let executor = context.create_basic_executor();
    /// # let node = executor.create_node("my_node").unwrap();
    /// use ros_env::example_interfaces::action::{
    ///     Fibonacci, Fibonacci_Feedback, Fibonacci_Goal, Fibonacci_Result,
    /// };
    ///
    /// async fn accept_beneath_100(goal: Arc<Fibonacci_Goal>) -> bool {
    ///     goal.order < 100
    /// }
    ///
    /// async fn execute_fibonacci_slowly(accepted: AcceptedGoal<Fibonacci>) -> TerminatedGoal {
    ///     let executing = match accepted.begin() {
    ///         BeginAcceptedGoal::Execute(executing) => executing,
    ///         BeginAcceptedGoal::Cancel(cancelled) => {
    ///             return cancelled.cancelled_with(Default::default());
    ///         }
    ///     };
    ///
    ///     let dt = Duration::from_secs(1);
    ///     let order = executing.goal().order;
    ///     let mut sequence = Vec::new();
    ///     let mut previous = 0;
    ///     let mut current = 1;
    ///     for _ in 0..order {
    ///         // Wait for 1s unless the action is cancelled.
    ///         let timer = smol::Timer::after(dt);
    ///         if executing.unless_cancel_requested(timer).await.is_err() {
    ///             // An err result means the action was cancelled, so let's
    ///             // stop right away.
    ///             return executing.begin_cancelling().cancelled_with(
    ///                 Fibonacci_Result { sequence }
    ///             );
    ///         }
    ///
    ///         sequence.push(previous);
    ///
    ///         let next = previous + current;
    ///         previous = current;
    ///         current = next;
    ///
    ///         // Publish our current progress
    ///         executing.publish_feedback(Fibonacci_Feedback {
    ///             sequence: sequence.clone(),
    ///         });
    ///     }
    ///
    ///     executing.succeeded_with(Fibonacci_Result { sequence })
    /// }
    ///
    /// let action_server = node.create_action_server(
    ///     "slow_fibonacci",
    ///     FifoActionQueue::<Fibonacci>::new().serve(
    ///         accept_beneath_100,
    ///         execute_fibonacci_slowly,
    ///     ),
    /// );
    /// ```
    pub fn serve<Accept, Acceptance, Execute, Execution>(
        &self,
        accept: Accept,
        execute: Execute,
    ) -> impl FnMut(RequestedGoal<Action>) -> BoxFuture<'static, TerminatedGoal>
    + use<Action, Accept, Acceptance, Execute, Execution>
    where
        Accept: FnMut(Arc<Action::Goal>) -> Acceptance + 'static + Send + Sync,
        Acceptance: Future<Output = bool> + 'static + Send + Sync,
        Execute: FnMut(AcceptedGoal<Action>) -> Execution + 'static + Send + Sync,
        Execution: Future<Output = TerminatedGoal> + 'static + Send + Sync,
    {
        let internal = Arc::clone(&self.internal);
        let accept = Arc::new(Mutex::new(accept));
        let execute = Arc::new(Mutex::new(execute));
        move |request: RequestedGoal<Action>| {
            let internal = Arc::clone(&internal);
            let receive = Arc::clone(&accept);
            let execute = Arc::clone(&execute);
            let future = async move {
                {
                    let mut receive = receive.lock().await;
                    if !receive(Arc::clone(request.goal())).await {
                        return request.reject();
                    }
                }

                let accepted = request.accept();

                let queued = {
                    let mut internal = internal.lock().await;
                    internal.add_to_queue()
                };

                if let Some(queued) = queued {
                    // Wait until we get a signal that it's our turn
                    let _ = queued.await;
                }

                let mut execute = execute.lock().await;
                let termination = execute(accepted).await;

                let mut internal = internal.lock().await;
                internal.next();

                termination
            };

            Box::pin(future)
        }
    }

    /// Implement an action server using this FIFO queue.
    ///
    /// All incoming goals will be immediately accepted, and then executed one
    /// at a time.
    pub fn accept_all<Execute, Execution>(
        &self,
        execute: Execute,
    ) -> impl FnMut(RequestedGoal<Action>) -> BoxFuture<'static, TerminatedGoal>
    + use<Action, Execute, Execution>
    where
        Execute: FnMut(AcceptedGoal<Action>) -> Execution + 'static + Send + Sync,
        Execution: Future<Output = TerminatedGoal> + 'static + Send + Sync,
    {
        self.serve(accept_all::<Action>, execute)
    }
}

async fn accept_all<A: ActionIDL>(_: Arc<A::Goal>) -> bool {
    true
}

#[cfg(test)]
mod tests {
    use crate::FifoActionQueue;
    use rclrs::*;
    use ros_env::example_interfaces::action::{
        Fibonacci, Fibonacci_Feedback, Fibonacci_Goal, Fibonacci_Result,
    };
    use futures::lock::Mutex;
    use std::sync::Arc;

    #[test]
    fn test_action_queue() {
        let context = Context::default();
        let mut executor = context.create_basic_executor();
        let node = executor.create_node(&format!("test_action_queue_node_{}", line!())).unwrap();

        let action_topic = format!("test_action_queue_topic_{}", line!());
        let (request_next, wait_for_next) = tokio::sync::mpsc::unbounded_channel();
        let wait_for_next = Arc::new(Mutex::new(wait_for_next));

        let action_server = node.create_action_server::<Fibonacci, _>(
            &action_topic,
            FifoActionQueue::<Fibonacci>::new().serve(
                move |request: Arc<Fibonacci_Goal>| {
                    async move {
                        request.order < 100
                    }
                },
                move |accepted: AcceptedGoal<_>| {
                    let wait_for_next = wait_for_next.clone();
                    fibonacci_test_server(wait_for_next, accepted)
                }
            ),
        ).unwrap();

        let action_client = node.create_action_client::<Fibonacci>(&action_topic).unwrap();

        let test_finished = executor.commands().run(async move {
            let client_for_notify = action_client.clone();
            let _ = node.notify_on_graph_change(move || client_for_notify.server_is_available().is_ok_and(|v| v)).await;

            let first_requested_goal = action_client.request_goal(Fibonacci_Goal {
                order: 3,
            });
            let invalid_requested_goal = action_client.request_goal(Fibonacci_Goal {
                order: 1000,
            });
            let second_requested_goal = action_client.request_goal(Fibonacci_Goal {
                order: 10,
            });
            let third_requested_goal = action_client.request_goal(Fibonacci_Goal {
                order: 10,
            });

            let mut first_goal = first_requested_goal.await.unwrap();

            // The invalid_goal will be rejected, so awaiting this will yield None
            assert!(invalid_requested_goal.await.is_none());

            let mut second_goal = second_requested_goal.await.unwrap();

            let mut third_goal = third_requested_goal.await.unwrap();

            assert!(first_goal.feedback.try_recv().is_err());
            assert!(second_goal.feedback.try_recv().is_err());
            assert!(third_goal.feedback.try_recv().is_err());

            first_goal.status.wait_for(|s| {
                s.code == GoalStatusCode::Executing
            }).await.unwrap();

            second_goal.status.wait_for(|s| {
                s.code == GoalStatusCode::Accepted
            }).await.unwrap();

            third_goal.status.wait_for(|s| {
                s.code == GoalStatusCode::Accepted
            }).await.unwrap();

            let _ = request_next.send(());
            assert_eq!(
                first_goal.feedback.recv().await.unwrap().sequence,
                vec![0],
            );
            assert!(second_goal.feedback.try_recv().is_err());
            assert!(third_goal.feedback.try_recv().is_err());

            let _ = request_next.send(());
            assert_eq!(
                first_goal.feedback.recv().await.unwrap().sequence,
                vec![0, 1],
            );
            assert!(second_goal.feedback.try_recv().is_err());
            assert!(third_goal.feedback.try_recv().is_err());

            let _ = request_next.send(());
            assert_eq!(
                first_goal.feedback.recv().await.unwrap().sequence,
                vec![0, 1, 1],
            );
            assert!(second_goal.feedback.try_recv().is_err());
            assert!(third_goal.feedback.try_recv().is_err());

            first_goal.status.wait_for(|s| {
                s.code == GoalStatusCode::Succeeded
            }).await.unwrap();

            let (code, result) = first_goal.result.await;
            assert!(code == GoalStatusCode::Succeeded);
            assert_eq!(result.sequence, vec![0, 1, 1]);

            second_goal.status.wait_for(|s| {
                s.code == GoalStatusCode::Executing
            }).await.unwrap();

            assert_eq!(third_goal.status.borrow().code, GoalStatusCode::Accepted);

            let _ = request_next.send(());
            assert_eq!(
                second_goal.feedback.recv().await.unwrap().sequence,
                vec![0],
            );
            assert!(third_goal.feedback.try_recv().is_err());

            let _ = request_next.send(());
            assert_eq!(
                second_goal.feedback.recv().await.unwrap().sequence,
                vec![0, 1],
            );
            assert!(third_goal.feedback.try_recv().is_err());

            assert!(second_goal.cancellation.cancel().await.is_accepted());
            let (code, result) = second_goal.result.await;
            assert_eq!(code, GoalStatusCode::Cancelled);
            assert_eq!(result.sequence, vec![0, 1]);

            third_goal.status.wait_for(|s| {
                s.code == GoalStatusCode::Executing
            }).await.unwrap();

            assert!(third_goal.feedback.try_recv().is_err());

            let mut previous = 0;
            let mut current = 1;
            let mut sequence = vec![];
            for _ in 0..10 {
                let _ = request_next.send(());
                let received_sequence = third_goal.feedback.recv().await.unwrap().sequence;

                sequence.push(previous);
                let next = previous + current;
                previous = current;
                current = next;

                assert_eq!(received_sequence, sequence);
            }

            let (code, result) = third_goal.result.await;
            assert_eq!(code, GoalStatusCode::Succeeded);
            assert_eq!(result.sequence, sequence);
        });

        executor.spin(SpinOptions::default().until_promise_resolved(test_finished));
        drop(action_server);
    }

    async fn fibonacci_test_server(
        wait_for_next: Arc<Mutex<tokio::sync::mpsc::UnboundedReceiver<()>>>,
        accepted: AcceptedGoal<Fibonacci>,
    ) -> TerminatedGoal {
        let executing = match accepted.begin() {
            BeginAcceptedGoal::Execute(executing) => executing,
            BeginAcceptedGoal::Cancel(cancelled) => {
                return cancelled.cancelled_with(Default::default());
            }
        };

        let mut sequence = Vec::new();
        let mut previous = 0;
        let mut current = 1;

        for _ in 0..executing.goal().order {
            if executing.unless_cancel_requested(wait_for_next.lock().await.recv()).await.is_err() {
                return executing.begin_cancelling().cancelled_with(Fibonacci_Result {
                    sequence,
                });
            }

            sequence.push(previous);

            let next = previous + current;
            previous = current;
            current = next;

            executing.publish_feedback(Fibonacci_Feedback {
                sequence: sequence.clone(),
            });
        }

        executing.succeeded_with(Fibonacci_Result { sequence })
    }

}
