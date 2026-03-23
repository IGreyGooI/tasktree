//! Async Behavior Tree Node Trait

use async_trait::async_trait;
use std::fmt::Debug;

use crate::types::{ActionResult, AsyncExecutionContext};

/// Core trait for async behavior tree action nodes
///
/// Actions are leaf nodes that do real work. They return only Success or Failure —
/// Running is synthesized by the runtime when it sees an in-flight JoinHandle.
#[async_trait]
pub trait AsyncBehaviorNode: Send + Sync + Debug + 'static {
    /// Execute the action asynchronously.
    ///
    /// - Read required state from the blackboard via `ctx`
    /// - Do the work (may be instant or take many ticks)
    /// - Return Success or Failure when done
    /// - Check `ctx.current_ct` cooperatively for cancellation
    async fn execute(&self, ctx: AsyncExecutionContext) -> ActionResult;

    /// Get the name of this node for debugging and logging
    fn name(&self) -> &str;
}