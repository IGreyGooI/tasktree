//! Selector node execution logic

use crate::tree::BehaviorTreeNode;
use crate::types::{AsyncExecutionContext, NodeResult};
use tracing::{debug, trace};

/// Execute selector node logic
///
/// A selector tries each child in order until one succeeds.
/// Returns Success if any child succeeds, Failure if all fail.
pub async fn execute_selector<CTX: Send + Sync + 'static>(
    name: &str,
    children: &[BehaviorTreeNode<CTX>],
    ctx: AsyncExecutionContext<CTX>,
) -> NodeResult {
    trace!("Executing selector: {}", name);

    for (i, child) in children.iter().enumerate() {
        if ctx.current_ct.is_cancelled() {
            debug!("Selector {} cancelled at child {}", name, i);
            return NodeResult::Failure;
        }

        trace!("Selector {} trying child {} ({})", name, i, child.name());

        let result = child.execute(ctx.child_context()).await;

        match result {
            NodeResult::Success => {
                debug!("Selector {} succeeded with child {}", name, i);
                return NodeResult::Success;
            }
            NodeResult::Failure => {
                trace!("Selector {} child {} failed, trying next", name, i);
                continue;
            }
            NodeResult::Running => {
                trace!("Selector {} child {} still running", name, i);
                return NodeResult::Running;
            }
        }
    }

    debug!("Selector {} failed - all children failed", name);
    NodeResult::Failure
}
