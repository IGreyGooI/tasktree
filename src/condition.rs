//! Condition trait for behavior tree conditions

use async_trait::async_trait;

use crate::blackboard::Blackboard;

/// Trait for behavior tree conditions
/// 
/// Conditions are used in condition nodes to determine which branch to execute.
#[async_trait]
pub trait Condition: Send + Sync + std::fmt::Debug +  'static {
    /// Evaluate the condition based on current blackboard state
    async fn evaluate(&self, blackboard: &Blackboard) -> bool;
    
    /// Get the name of this condition for debugging
    fn name(&self) -> &str;
}