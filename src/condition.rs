//! Condition trait for behavior tree conditions

use async_trait::async_trait;

use crate::types::AsyncExecutionContext;

/// Trait for behavior tree conditions.
///
/// `CTX` is the same engine-specific user context as in `AsyncBehaviorNode<CTX>`.
/// Conditions receive the full execution context so they can inspect `ctx.user`
/// as well as `ctx.blackboard`.
///
/// For trees built without a user context, use `CTX = ()`.
#[async_trait]
pub trait Condition<CTX>: Send + Sync + std::fmt::Debug + 'static
where
    CTX: Send + Sync + 'static,
{
    /// Evaluate the condition based on the current execution context.
    async fn evaluate(&self, ctx: &AsyncExecutionContext<CTX>) -> bool;

    /// Get the name of this condition for debugging
    fn name(&self) -> &str;
}
