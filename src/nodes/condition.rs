//! Condition node execution logic

use crate::tree::BehaviorTreeNode;
use crate::types::{AsyncExecutionContext, NodeResult};
use crate::condition::Condition;
use tracing::{debug, trace};

/// Execute condition node logic
///
/// A condition evaluates a predicate and executes either true_branch or false_branch.
pub async fn execute_condition(
    name: &str,
    condition: &std::sync::Arc<dyn Condition>,
    true_branch: &Box<BehaviorTreeNode>,
    false_branch: &Option<Box<BehaviorTreeNode>>,
    ctx: AsyncExecutionContext,
) -> NodeResult {
    trace!("Executing condition: {}", name);
    
    // Check cancellation before starting
    if ctx.current_ct.is_cancelled() {
        debug!("Condition {} cancelled before execution", name);
        return NodeResult::Failure; // Return failure when cancelled
    }
    
    // Evaluate the condition
    let condition_result = {
        condition.evaluate(&ctx.blackboard).await
    };
    
    trace!("Condition {} evaluated to: {}", name, condition_result);
    
    // Choose which branch to execute based on condition result
    let branch_to_execute = if condition_result {
        Some(true_branch.as_ref())
    } else {
        false_branch.as_ref().map(|b| b.as_ref())
    };
    
    match branch_to_execute {
        Some(branch) => {
            debug!("Condition {} executing {} branch", name, if condition_result { "true" } else { "false" });
            
            // Check cancellation before executing branch
            if ctx.current_ct.is_cancelled() {
                debug!("Condition {} cancelled before branch execution", name);
                return NodeResult::Failure;
            }
            
            // Execute the selected branch
            ctx.execute(branch).await
        }
        None => {
            // No branch to execute - return success if condition was true, failure if false
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