//! Parallel node execution logic with multi-frame support

use crate::tree::BehaviorTreeNode;
use crate::types::{AsyncExecutionContext, NodeResult, ParallelPolicy};
use tracing::{debug, trace};

/// Execute parallel node logic with multi-frame support
/// 
/// A parallel executes all children every tick and applies policy based on current results.
/// Children maintain their own state across frames.
/// 
pub async fn execute_parallel(
    name: &str,
    children: &[BehaviorTreeNode],
    policy: ParallelPolicy,
    ctx: AsyncExecutionContext,
) -> NodeResult {
    trace!("Executing parallel: {} with policy: {:?}", name, policy);
    
    if children.is_empty() {
        debug!("Parallel {} has no children, returning Success", name);
        return NodeResult::Success;
    }
    
    // Check cancellation early
    if ctx.current_ct.is_cancelled() {
        debug!("Parallel {} cancelled", name);
        return NodeResult::Failure;
    }
    
    // Execute all children and count results for this tick
    let mut success_count = 0;
    let mut failure_count = 0;
    let mut running_count = 0;
    let total_children = children.len();
    
    debug!("Parallel {} executing {} children", name, total_children);
    
    // Execute all children
    for (i, child) in children.iter().enumerate() {
        let child_ctx = ctx.child_context();
        let result = child_ctx.execute(child).await;
        
        trace!("Parallel {} child {} ({}) result: {:?}", name, i, child.name(), result);
        
        match result {
            NodeResult::Success => success_count += 1,
            NodeResult::Failure => failure_count += 1,
            NodeResult::Running => running_count += 1,
        }
    }
    
    debug!("Parallel {} tick results: {} success, {} failure, {} running", 
           name, success_count, failure_count, running_count);
    
    // Apply policy logic based on this tick's results
    match policy {
        ParallelPolicy::AllSucceed => {
            if failure_count > 0 {
                debug!("Parallel {} failed - child failed (policy: AllSucceed)", name);
                NodeResult::Failure
            } else if success_count == total_children {
                debug!("Parallel {} succeeded - all children succeeded (policy: AllSucceed)", name);
                NodeResult::Success
            } else {
                trace!("Parallel {} continuing - {}/{} succeeded, {} running (policy: AllSucceed)", 
                       name, success_count, total_children, running_count);
                NodeResult::Running
            }
        }
        
        ParallelPolicy::FirstSucceed => {
            if success_count > 0 {
                debug!("Parallel {} succeeded - first child succeeded (policy: FirstSucceed)", name);
                NodeResult::Success
            } else if running_count == 0 {
                // All completed with no success
                debug!("Parallel {} failed - no children succeeded (policy: FirstSucceed)", name);
                NodeResult::Failure
            } else {
                trace!("Parallel {} continuing - waiting for first success, {} running (policy: FirstSucceed)", 
                       name, running_count);
                NodeResult::Running
            }
        }
        
        ParallelPolicy::AnySucceed => {
            if running_count > 0 {
                trace!("Parallel {} continuing - {} children still running (policy: AnySucceed)", 
                       name, running_count);
                NodeResult::Running
            } else if success_count > 0 {
                debug!("Parallel {} succeeded - {} children succeeded (policy: AnySucceed)", 
                       name, success_count);
                NodeResult::Success
            } else {
                debug!("Parallel {} failed - no children succeeded (policy: AnySucceed)", name);
                NodeResult::Failure
            }
        }
    }
}