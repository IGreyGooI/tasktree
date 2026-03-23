//! Sequence node execution logic

use crate::tree::BehaviorTreeNode;
use crate::types::{AsyncExecutionContext, NodeResult};
use tracing::{debug, trace};

/// Execute sequence node logic
/// 
/// A sequence executes each child in order until one fails.
/// Returns Success if all children succeed, Failure if any fails, Cancelled if interrupted.
pub async fn execute_sequence(
    name: &str,
    children: &[BehaviorTreeNode],
    ctx: AsyncExecutionContext,
) -> NodeResult {
    trace!("Executing sequence: {}", name);
    
    // Execute each child until one fails
    for (i, child) in children.iter().enumerate() {
        // Check cancellation before each child
        if ctx.current_ct.is_cancelled() {
            debug!("Sequence {} cancelled at child {}", name, i);
            return NodeResult::Failure; // Return failure when cancelled
        }
        
        trace!("Sequence {} executing child {} ({})", name, i, child.name());
        
        // Execute child with context
        let result = ctx.execute(child).await;
        
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
