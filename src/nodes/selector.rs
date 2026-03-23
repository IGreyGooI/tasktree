//! Selector node execution logic

use crate::tree::BehaviorTreeNode;
use crate::types::{AsyncExecutionContext, NodeResult};
use tracing::{debug, trace};

/// Execute selector node logic
/// 
/// A selector tries each child in order until one succeeds.
/// Returns Success if any child succeeds, Failure if all fail, Cancelled if interrupted.
pub async fn execute_selector(
    name: &str,
    children: &[BehaviorTreeNode],
    ctx: AsyncExecutionContext,
) -> NodeResult {
    trace!("Executing selector: {}", name);
    
    // Try each child until one succeeds
    for (i, child) in children.iter().enumerate() {
        // Check cancellation before each child
        if ctx.current_ct.is_cancelled() {
            debug!("Selector {} cancelled at child {}", name, i);
            return NodeResult::Failure; // Return failure when cancelled
        }
        
        trace!("Selector {} trying child {} ({})", name, i, child.name());
        
        // Execute child with context
        let result = ctx.execute(child).await;
        
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