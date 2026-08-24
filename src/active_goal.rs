use rclrs::{ActionIDL, CancellingGoal, ExecutingGoal, FeedbackPublisher, TerminatedGoal};
use std::sync::Arc;

/// Represents a goal that is still active, either executing or cancelling.
pub enum ActiveGoal<A: ActionIDL> {
    Executing(ExecutingGoal<A>),
    Cancelling(CancellingGoal<A>),
}

impl<A: ActionIDL> ActiveGoal<A> {
    /// Check if the goal is in a cancelling state
    pub fn is_cancelling(&self) -> bool {
        matches!(self, Self::Cancelling(_))
    }

    /// Get the goal of this action.
    pub fn goal(&self) -> &Arc<A::Goal> {
        match self {
            Self::Executing(e) => e.goal(),
            Self::Cancelling(c) => c.goal(),
        }
    }

    /// Same as [`ExecutingGoal::until_cancel_requested`] unless the goal is
    /// already in a cancelling state, in which case the future is just returned
    /// immediately as an Err.
    pub async fn until_cancel_requested<F: Future + Unpin>(&self, f: F) -> Result<F::Output, F> {
        match self {
            Self::Executing(e) => e.until_cancel_requested(f).await,
            Self::Cancelling(_) => Err(f),
        }
    }

    /// Same as [`ExecutingGoal::unless_cancel_requested`] unless the goal is
    /// already in a cancelling state, in which case the future is just immediately
    /// dropped.
    pub async fn unless_cancel_requested<F: Future>(&self, f: F) -> Result<F::Output, ()> {
        match self {
            Self::Executing(e) => e.unless_cancel_requested(f).await,
            Self::Cancelling(_) => Err(()),
        }
    }

    /// Transition to a cancelling if the goal is currently executing. If the
    /// goal is already cancelling then just receive its current cancelling state.
    #[must_use]
    pub fn begin_cancelling(self) -> CancellingGoal<A> {
        match self {
            Self::Executing(e) => e.begin_cancelling(),
            Self::Cancelling(c) => c,
        }
    }

    /// If the goal is being executed, reject any open cancellation requests.
    pub fn reject_cancellation(&self) {
        if let Self::Executing(e) = self {
            e.reject_cancellation();
        }
    }

    /// Transition the goal into the succeeded state.
    ///
    /// "Succeeded" is a terminal state, so the state of the goal can no longer
    /// be changed after this. Publish all relevant feedback before calling this.
    pub fn succeeded_with(self, result: A::Result) -> TerminatedGoal {
        match self {
            Self::Executing(e) => e.succeeded_with(result),
            Self::Cancelling(c) => c.succeeded_with(result),
        }
    }

    /// Transition the goal into the aborted state.
    pub fn aborted_with(self, result: A::Result) -> TerminatedGoal {
        match self {
            Self::Executing(e) => e.aborted_with(result),
            Self::Cancelling(c) => c.aborted_with(result),
        }
    }

    /// Terminate the goal with a cancelled state. If the goal is still executing,
    /// it will transition to cancelling and then cancelled.
    pub fn cancelled_with(self, result: A::Result) -> TerminatedGoal {
        match self {
            Self::Executing(e) => e.begin_cancelling().cancelled_with(result),
            Self::Cancelling(c) => c.cancelled_with(result),
        }
    }

    /// Publish feedback for the action clients to read.
    pub fn publish_feedback(&self, feedback: A::Feedback) {
        match self {
            Self::Executing(e) => e.publish_feedback(feedback),
            Self::Cancelling(c) => c.publish_feedback(feedback),
        }
    }

    /// Get a handler specifically for publishing feedback for this goal.
    pub fn feedback_publisher(&self) -> FeedbackPublisher<A> {
        match self {
            Self::Executing(e) => e.feedback_publisher(),
            Self::Cancelling(c) => c.feedback_publisher(),
        }
    }
}
