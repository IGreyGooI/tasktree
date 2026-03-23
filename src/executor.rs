//! Async Behavior Tree Executor

use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::blackboard::Blackboard;
use crate::types::{AsyncExecutionContext, NodeResult};

/// Simple async behavior tree executor
/// 
/// Responsible for:
/// - Creating execution contexts for nodes
/// - Managing the overall execution lifecycle  
/// - Handling cancellation at the executor level
pub struct AsyncBehaviorTreeExecutor {
    /// The root node to execute
    root: crate::tree::BehaviorTreeNode,
    /// Shared blackboard
    blackboard: Blackboard,
    /// Executor-level cancellation token
    cancellation_token: CancellationToken,
    /// Executor identifier
    executor_id: String,
}

impl AsyncBehaviorTreeExecutor {
    /// Create a new executor
    pub fn new(
        root: crate::tree::BehaviorTreeNode,
        blackboard: &Blackboard,
        executor_id: String,
    ) -> Self {
        Self {
            root,
            blackboard: blackboard.clone(),
            cancellation_token: CancellationToken::new(),
            executor_id,
        }
    }

    /// Execute the behavior tree once
    /// 
    /// This creates a fresh execution context and runs the root node.
    /// The executor decides what context to provide to nodes.
    pub async fn execute_once(&self) -> NodeResult {
        debug!("Executing behavior tree: {}", self.executor_id);

        // Create execution context - this is what the executor provides to nodes
        let ctx = AsyncExecutionContext {
            blackboard: self.blackboard.clone(),
            current_ct: self.cancellation_token.clone(),
        };

        // Execute the root node using the context's execute method
        let result = ctx.execute(&self.root).await;

        match result {
            NodeResult::Success => {
                debug!("Behavior tree {} completed successfully", self.executor_id);
            }
            NodeResult::Failure => {
                debug!("Behavior tree {} failed", self.executor_id);
            }
            NodeResult::Running => {
                debug!("Behavior tree {} still running", self.executor_id);
            }
        }

        result
    }

    /// Cancel the executor (and all running nodes)
    pub fn cancel(&self) {
        info!("Cancelling behavior tree executor: {}", self.executor_id);
        self.cancellation_token.cancel();
    }

    /// Check if the executor is cancelled
    pub fn is_cancelled(&self) -> bool {
        self.cancellation_token.is_cancelled()
    }

    /// Get access to the blackboard
    pub fn blackboard(&self) -> Blackboard {
        self.blackboard.clone()
    }
}

impl Drop for AsyncBehaviorTreeExecutor {
    fn drop(&mut self) {
        warn!("Dropping behavior tree executor: {}", self.executor_id);
        self.cancellation_token.cancel();
    }
}