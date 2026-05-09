//! Core data structures for async behavior tree

use crate::blackboard::Blackboard;
use serde::Serialize;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

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

/// Execution context passed to async behavior nodes and conditions on every tick.
///
/// `CTX` is the engine-specific user context (e.g. `EngineContext { world, npc_id }`).
/// The tasktree library itself is agnostic to what `CTX` contains.  Use `CTX = ()`
/// for trees that do not need a user context.
///
/// # Fields
///
/// * [`blackboard`](Self::blackboard) — shared key/value store for cross-node communication
/// * [`current_ct`](Self::current_ct) — cooperative cancellation signal; thread it into
///   every long `.await` your action does so cancellation reaches in-flight work
/// * [`user`](Self::user) — engine context, cloned cheaply via `Arc`
///
/// # Example
///
/// ```rust,ignore
/// async fn execute(&self, ctx: AsyncExecutionContext<MyCtx>) -> ActionResult {
///     // Read shared state from the user context.
///     let world = ctx.user.world.clone();
///
///     // Long async work — pass the cancellation token down.
///     let result = call_llm(&world, &ctx.current_ct).await;
///
///     match result {
///         Ok(v) => { /* apply, emit events */ ActionResult::Success }
///         Err(_) => ActionResult::Failure,
///     }
/// }
/// ```
#[derive(Clone)]
pub struct AsyncExecutionContext<CTX> {
    /// Shared blackboard for state management.
    pub blackboard: Blackboard,
    /// Cooperative cancellation token for this execution scope.
    /// Pass this into every long `.await` an action makes.
    pub current_ct: CancellationToken,
    /// Engine-injected context (world handle, npc id, …).
    /// Cloned cheaply via `Arc` on every child context.
    pub user: Arc<CTX>,
}

impl<CTX: std::fmt::Debug> std::fmt::Debug for AsyncExecutionContext<CTX> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AsyncExecutionContext")
            .field("blackboard", &self.blackboard)
            .field("current_ct", &self.current_ct)
            .field("user", &self.user)
            .finish()
    }
}

impl<CTX: Send + Sync + 'static> AsyncExecutionContext<CTX> {
    /// Create a new execution context.
    pub fn new(
        blackboard: Blackboard,
        cancellation_token: CancellationToken,
        user: Arc<CTX>,
    ) -> Self {
        Self {
            blackboard,
            current_ct: cancellation_token,
            user,
        }
    }

    /// Create a child context that inherits parent cancellation.
    /// `Arc::clone` on `user` is cheap.
    pub fn child_context(&self) -> Self {
        Self {
            blackboard: self.blackboard.clone(),
            current_ct: self.current_ct.child_token(),
            user: Arc::clone(&self.user),
        }
    }
}

/// Parallel execution policy
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, serde::Deserialize, Default)]
pub enum ParallelPolicy {
    /// All children must succeed (parallel sequence behavior)
    #[default]
    AllSucceed,
    /// First child to succeed wins, others are cancelled (parallel selector behavior)
    FirstSucceed,
    /// At least one child must succeed, all run to completion (parallel optional behavior)
    AnySucceed,
}

/// Task handle for tracking parallel execution
#[derive(Debug, serde::Serialize)]
pub struct TaskHandle {
    #[serde(skip)] // JoinHandle doesn't implement Serialize
    pub handle: tokio::task::JoinHandle<NodeResult>,
    pub started_at: std::time::SystemTime,
    pub task_name: String,
}
