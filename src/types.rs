//! Core data structures for async behavior tree

use std::fmt::Debug;
use serde::Serialize;
use tokio_util::sync::CancellationToken;
use tracing::debug;
use crate::blackboard::Blackboard;

/// Result returned by action leaf nodes — no Running, that is the runtime's job
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ActionResult {
    Success,
    Failure,
}

/// Result of executing a behavior tree node (composite or action via runtime)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeResult {
    /// The node completed successfully
    Success,
    /// The node failed due to logical reasons
    Failure,
    /// The node is still running and needs more time (multi-frame execution)
    Running,
}

impl From<ActionResult> for NodeResult {
    fn from(r: ActionResult) -> Self {
        match r {
            ActionResult::Success => NodeResult::Success,
            ActionResult::Failure => NodeResult::Failure,
        }
    }
}

/// Execution context passed to async behavior nodes
#[derive(Debug, Clone)]
pub struct AsyncExecutionContext {
    /// Shared blackboard for state management
    pub blackboard: Blackboard,
    /// Current cancellation token for this execution scope
    pub current_ct: CancellationToken,
}

impl AsyncExecutionContext {
    /// Create a new execution context
    pub fn new(blackboard: Blackboard, cancellation_token: CancellationToken) -> Self {
        Self {
            blackboard,
            current_ct: cancellation_token,
        }
    }

    /// Execute a child node with this context (stack-based execution)
    #[track_caller]
    pub fn execute<'a>(&'a self, node: &'a crate::tree::BehaviorTreeNode) -> std::pin::Pin<Box<dyn std::future::Future<Output = NodeResult> + Send + 'a>> {
        // Create child context with hierarchical cancellation
        let child_context = self.child_context();
        debug!("Executing node: {}", node.name());
        Box::pin(async move {
            match node {
                crate::tree::BehaviorTreeNode::Action { node, .. } => {
                    node.execute(child_context).await.into()
                }
                crate::tree::BehaviorTreeNode::Sequence { name, children, .. } => {
                    crate::nodes::sequence::execute_sequence(name, children, child_context).await
                }
                crate::tree::BehaviorTreeNode::Selector { name, children, .. } => {
                    crate::nodes::selector::execute_selector(name, children, child_context).await
                }
                crate::tree::BehaviorTreeNode::Parallel { name, children, policy, .. } => {
                    crate::nodes::parallel::execute_parallel(name, children, *policy, child_context).await
                }
                crate::tree::BehaviorTreeNode::Condition { name, condition, true_branch, false_branch, .. } => {
                    crate::nodes::condition::execute_condition(name, condition, true_branch, false_branch, child_context).await
                }
            }
        })
    }

    /// Create a child context that inherits parent cancellation
    pub fn child_context(&self) -> Self {
        Self {
            blackboard: self.blackboard.clone(),
            current_ct: self.current_ct.child_token(),
        }
    }
}

/// Parallel execution policy
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, serde::Deserialize)]
pub enum ParallelPolicy {
    /// All children must succeed (parallel sequence behavior)
    AllSucceed,
    /// First child to succeed wins, others are cancelled (parallel selector behavior)
    FirstSucceed,
    /// At least one child must succeed, all run to completion (parallel optional behavior)
    AnySucceed,
}

impl Default for ParallelPolicy {
    fn default() -> Self {
        ParallelPolicy::AllSucceed
    }
}

/// Task handle for tracking parallel execution
#[derive(Debug, serde::Serialize)]
pub struct TaskHandle {
    #[serde(skip)] // JoinHandle doesn't implement Serialize
    pub handle: tokio::task::JoinHandle<NodeResult>,
    pub started_at: std::time::SystemTime,
    pub task_name: String,
}