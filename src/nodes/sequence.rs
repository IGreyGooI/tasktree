//! Sequence node execution logic

use crate::tree::BehaviorTreeNode;
use crate::types::{AsyncExecutionContext, NodeResult};
use tracing::{debug, trace};

/// Execute sequence node logic
///
/// A sequence executes each child in order until one fails.
/// Returns Success if all children succeed, Failure if any fails.
pub async fn execute_sequence<CTX: Send + Sync + 'static>(
    name: &str,
    children: &[BehaviorTreeNode<CTX>],
    ctx: AsyncExecutionContext<CTX>,
) -> NodeResult {
    trace!("Executing sequence: {}", name);

    for (i, child) in children.iter().enumerate() {
        if ctx.current_ct.is_cancelled() {
            debug!("Sequence {} cancelled at child {}", name, i);
            return NodeResult::Failure;
        }

        trace!("Sequence {} executing child {} ({})", name, i, child.name());

        let result = child.execute(ctx.child_context()).await;

        match result {
            NodeResult::Success => {
                trace!("Sequence {} child {} succeeded, continuing", name, i);
                continue;
            }
            NodeResult::Failure => {
                debug!("Sequence {} failed at child {}", name, i);
                return NodeResult::Failure;
            }
            NodeResult::Running => {
                trace!("Sequence {} child {} still running", name, i);
                return NodeResult::Running;
            }
        }
    }

    debug!("Sequence {} succeeded - all children succeeded", name);
    NodeResult::Success
}
