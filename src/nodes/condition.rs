//! Condition node execution logic

use crate::tree::BehaviorTreeNode;
use crate::types::{AsyncExecutionContext, NodeResult};
use crate::condition::Condition;
use std::sync::Arc;
use tracing::{debug, trace};

/// Execute condition node logic
///
/// A condition evaluates a predicate and executes either true_branch or false_branch.
pub async fn execute_condition<CTX: Send + Sync + 'static>(
    name: &str,
    condition: &Arc<dyn Condition<CTX>>,
    true_branch: &Box<BehaviorTreeNode<CTX>>,
    false_branch: &Option<Box<BehaviorTreeNode<CTX>>>,
    ctx: AsyncExecutionContext<CTX>,
) -> NodeResult {
    trace!("Executing condition: {}", name);

    if ctx.current_ct.is_cancelled() {
        debug!("Condition {} cancelled before execution", name);
        return NodeResult::Failure;
    }

    let condition_result = condition.evaluate(&ctx).await;

    trace!("Condition {} evaluated to: {}", name, condition_result);

    let branch_to_execute = if condition_result {
        Some(true_branch.as_ref())
    } else {
        false_branch.as_ref().map(|b| b.as_ref())
    };

    match branch_to_execute {
        Some(branch) => {
            debug!("Condition {} executing {} branch", name, if condition_result { "true" } else { "false" });

            if ctx.current_ct.is_cancelled() {
                debug!("Condition {} cancelled before branch execution", name);
                return NodeResult::Failure;
            }

            branch.execute(ctx.child_context()).await
        }
        None => {
            let result = if condition_result {
                NodeResult::Success
            } else {
                NodeResult::Failure
            };
            debug!("Condition {} has no {} branch, returning {:?}",
                   name,
                   if condition_result { "true" } else { "false" },
                   result);
            result
        }
    }
}
