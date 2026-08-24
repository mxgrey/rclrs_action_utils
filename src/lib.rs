pub mod fifo_queue;
pub use fifo_queue::*;

pub mod fifo_action_queue;
pub use fifo_action_queue::*;

pub mod active_goal;
pub use active_goal::*;

#[cfg(test)]
mod tests {
    use crate::*;
    use futures::lock::Mutex;
    use rclrs::*;
    use ros_env::example_interfaces::action::{
        Fibonacci as MyAction, Fibonacci_Goal, Fibonacci_Result,
    };
    use std::sync::Arc;

    #[test]
    fn test_simplest_action() {
        let context = Context::default();
        let executor = context.create_basic_executor();
        let node = executor
            .create_node(&format!("test_simple_action_{}", line!()))
            .unwrap();

        let _action_server = node.create_action_server("my_action", handle_request);

        async fn handle_request(request: RequestedGoal<MyAction>) -> TerminatedGoal {
            request.reject()
        }
    }

    #[test]
    fn test_accept_execute_chain_action() {
        let context = Context::default();
        let executor = context.create_basic_executor();
        let node = executor
            .create_node(&format!("test_simple_action_{}", line!()))
            .unwrap();

        let _action_server = node.create_action_server("my_action", handle_request);

        async fn handle_request(request: RequestedGoal<MyAction>) -> TerminatedGoal {
            let execution = request.accept().execute();

            /* ... Do something ... */

            execution.succeeded_with(Default::default())
        }
    }

    use ros_env::example_interfaces::action::{
        Fibonacci as Fetch, Fibonacci as Navigate, Fibonacci as Pick, Fibonacci as Place,
        Fibonacci_Goal as Navigate_Goal,
    };

    #[test]
    fn test_composed_action() {
        let context = Context::default();
        let executor = context.create_basic_executor();
        let node = executor
            .create_node(&format!("test_simple_action_{}", line!()))
            .unwrap();

        let clients = FetchingClients {
            navigate: node.create_action_client::<Navigate>("navigate").unwrap(),
            pick: node.create_action_client::<Pick>("pick").unwrap(),
            place: node.create_action_client::<Place>("place").unwrap(),
        };

        let handle_request = move |request: RequestedGoal<Fetch>| {
            let clients = clients.clone();
            async move {
                let execution = request.accept().execute();
                let pickup_location = get_pickup_location(execution.goal());
                let Some(navigation) = clients.navigate.request_goal(pickup_location).await else {
                    return execution.aborted_with(Default::default());
                };

                if navigation.result.await.0 != GoalStatusCode::Succeeded {
                    // TODO: Do better error handling
                    return execution.aborted_with(Default::default());
                }

                /* ... TODO: execute other actions ... */

                return execution.succeeded_with(Default::default());
            }
        };

        let _action_server = node.create_action_server("my_action", handle_request);
    }

    #[test]
    fn test_cancelling_action() {
        let context = Context::default();
        let executor = context.create_basic_executor();
        let node = executor
            .create_node(&format!("test_simple_action_{}", line!()))
            .unwrap();

        let clients = FetchingClients {
            navigate: node.create_action_client::<Navigate>("navigate").unwrap(),
            pick: node.create_action_client::<Pick>("pick").unwrap(),
            place: node.create_action_client::<Place>("place").unwrap(),
        };

        let handle_request = move |request: RequestedGoal<Fetch>| {
            let clients = clients.clone();
            async move {
                let execution = request.accept().execute();
                let pickup_location = get_pickup_location(execution.goal());
                let Some(navigation) = clients.navigate.request_goal(pickup_location).await else {
                    return execution.aborted_with(Default::default());
                };

                match execution.until_cancel_requested(navigation.result).await {
                    Ok((nav_status, nav_response)) => {
                        if nav_status == GoalStatusCode::Aborted {
                            return execution.aborted_with(nav_response);
                        }
                    }
                    Err(nav_result) => {
                        let cancelling = execution.begin_cancelling();
                        navigation.cancellation.cancel();
                        let (_, nav_response) = nav_result.await;
                        return cancelling.cancelled_with(nav_response);
                    }
                };

                /* ... TODO: execute other actions ... */

                return execution.succeeded_with(Default::default());
            }
        };

        let _action_server = node.create_action_server("my_action", handle_request);
    }

    #[derive(Clone)]
    struct FetchingClients {
        #[allow(unused)]
        navigate: ActionClient<Navigate>,
        #[allow(unused)]
        pick: ActionClient<Pick>,
        #[allow(unused)]
        place: ActionClient<Place>,
    }

    fn get_pickup_location(goal: &Arc<Fibonacci_Goal>) -> Fibonacci_Goal {
        (**goal).clone()
    }

    #[test]
    fn test_navigation_example() {
        let context = Context::default();
        let executor = context.create_basic_executor();
        let node = executor
            .create_node(&format!("test_navigation_action_{}", line!()))
            .unwrap();
        let final_location = Arc::new(Mutex::new(Location {}));

        let nav_accept = move |goal: Arc<Navigate_Goal>| {
            let final_location = final_location.clone();
            async move {
                let mut start_location = final_location.lock().await;
                match generate_plan(*start_location, goal).await {
                    Some(plan) => {
                        *start_location = plan.end_location;
                        return Some(plan);
                    }
                    None => return None,
                }
            }
        };

        async fn nav_execute(goal: AcceptedGoal<Navigate>, plan: Plan) -> TerminatedGoal {
            let execution = match goal.begin() {
                BeginAcceptedGoal::Execute(execution) => execution,
                BeginAcceptedGoal::Cancel(cancel) => {
                    return cancel.cancelled_with(Default::default());
                }
            };

            let result = follow_plan(plan).await;
            return execution.succeeded_with(result);
        }

        let _action_server = node.create_action_server(
            "navigate",
            FifoActionQueue::new().serve(nav_accept, nav_execute),
        );
    }

    async fn generate_plan(_current: Location, _goal: Arc<Navigate_Goal>) -> Option<Plan> {
        Some(Plan {
            end_location: Location {},
        })
    }

    async fn follow_plan(_plan: Plan) -> Fibonacci_Result {
        Default::default()
    }

    #[derive(Clone, Copy)]
    struct Location {}

    struct Plan {
        end_location: Location,
    }

    #[test]
    fn test_action_client() {
        let context = Context::default();
        let executor = context.create_basic_executor();
        let node = executor
            .create_node(&format!("test_multiple_goals_{}", line!()))
            .unwrap();

        let client = node
            .create_action_client::<Navigate>("test_action_client")
            .unwrap();
        let _ = executor.commands().run(async move {
            let _ = client.notify_on_server_ready().await;
            let Some(goal) = client.request_goal(create_navigation_goal()).await else {
                log_error!("requesting_navigation", "Request rejected!");
                return None;
            };

            use futures::StreamExt;
            let mut goal_stream = goal.stream();
            while let Some(next) = goal_stream.next().await {
                match next {
                    GoalEvent::Status(status) => {
                        println!("A new status update: {status:?}");
                    }
                    GoalEvent::Feedback(feedback) => {
                        println!("A new feedback message: {feedback:?}");
                    }
                    GoalEvent::Result((status, response)) => {
                        println!("Finished with status: {status:?}");
                        return Some(response);
                    }
                }
            }

            return None;
        });
    }

    fn create_navigation_goal() -> Navigate_Goal {
        Navigate_Goal::default()
    }
}
