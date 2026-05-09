//! Async behavior tree runtime for Rust.
//!
//! ## Quick start
//!
//! ```ignore
//! use tasktree::{BehaviorTreeRuntime, BtRegistry, BehaviorTreeNode, AsyncExecutionContext, ActionResult};
//! use async_trait::async_trait;
//! use std::sync::Arc;
//!
//! // 1. Define your user context
//! struct MyCtx { world: MyWorld }
//!
//! // 2. Implement action nodes
//! #[derive(Debug)]
//! struct MyAction;
//!
//! #[async_trait]
//! impl AsyncBehaviorNode<MyCtx> for MyAction {
//!     async fn execute(&self, ctx: AsyncExecutionContext<MyCtx>) -> ActionResult {
//!         ctx.user.world.do_something();
//!         ActionResult::Success
//!     }
//!     fn name(&self) -> &str { "MyAction" }
//! }
//!
//! // 3. Build a registry
//! let mut reg = BtRegistry::<MyCtx>::new();
//! reg.register_action(|| Arc::new(MyAction));
//!
//! // 4. Load a tree (YAML) or build one with the builder
//! let mut rt = BehaviorTreeRuntime::from_yaml(yaml, Blackboard::new(), Arc::new(ctx), &reg)?;
//!
//! // 5. Tick
//! rt.tick().await;
//! ```

pub mod blackboard;
pub mod builder;
pub mod condition;
pub mod error;
#[cfg(feature = "lua")]
pub mod lua;
pub mod node;
pub mod nodes;
pub mod poll;
pub mod registry;
pub mod runtime;
pub mod tree;
pub mod tree_def;
pub mod types;
pub mod utils;

// Re-exports for convenience
pub use blackboard::Blackboard;
pub use builder::{BehaviorTreeBuilder, FunctionNode, IntoAsyncBehaviorNode};
pub use condition::Condition;
pub use error::RobotBTError;
pub use node::AsyncBehaviorNode;
pub use registry::BtRegistry;
pub use runtime::BehaviorTreeRuntime;
pub use tree::BehaviorTreeNode;
pub use types::{ActionResult, AsyncExecutionContext, NodeResult, ParallelPolicy};
