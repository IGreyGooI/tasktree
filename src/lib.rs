//! Async behavior tree runtime for Rust.

// Re-export tracing for convenience
pub use tracing;

// Modules
pub mod types;
pub mod blackboard;
pub mod error;
pub mod node;
pub mod executor;
pub mod tree;
pub mod nodes;
pub mod condition;
pub mod builder;
pub mod registry;
pub mod tree_def;
#[cfg(feature = "lua")]
pub mod lua;
// pub mod http_api;
// pub mod websocket;
pub mod poll;
pub mod utils;
pub mod runtime;

// Re-export inventory so macros in registry.rs can reference it as $crate::inventory
pub use inventory;

// Re-exports for convenience
pub use blackboard::Blackboard;
pub use error::RobotBTError;
pub use builder::{BehaviorTreeBuilder, IntoAsyncBehaviorNode, FunctionNode};
pub use registry::{registered_actions, registered_conditions};