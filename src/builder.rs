//! Behavior Tree Builder — fluent API for constructing behavior trees

use crate::{
    condition::Condition,
    node::AsyncBehaviorNode,
    tree::{BehaviorTreeNode, NodeId},
    types::{ActionResult, AsyncExecutionContext, ParallelPolicy},
};
use std::{any::type_name_of_val, future::Future};
use std::sync::Arc;

/// Wrapper for plain async functions used as action nodes.
///
/// `CTX` must be `Send + Sync + 'static`.
pub struct FunctionNode<CTX, F> {
    name: String,
    func: F,
    _ctx: std::marker::PhantomData<CTX>,
}

impl<CTX, F> FunctionNode<CTX, F> {
    pub fn new<S: Into<String>>(name: S, func: F) -> Self {
        Self {
            name: name.into(),
            func,
            _ctx: std::marker::PhantomData,
        }
    }
}

impl<CTX, F> std::fmt::Debug for FunctionNode<CTX, F> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FunctionNode")
            .field("name", &self.name)
            .field("func", &type_name_of_val(&self.func))
            .finish()
    }
}

#[async_trait::async_trait]
impl<CTX, F, Fut> AsyncBehaviorNode<CTX> for FunctionNode<CTX, F>
where
    CTX: Send + Sync + 'static,
    F: Fn(AsyncExecutionContext<CTX>) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = ActionResult> + Send + 'static,
{
    async fn execute(&self, ctx: AsyncExecutionContext<CTX>) -> ActionResult {
        (self.func)(ctx).await
    }

    fn name(&self) -> &str {
        &self.name
    }
}

// ---------------------------------------------------------------------------
// IntoAsyncBehaviorNode — conversion trait
// ---------------------------------------------------------------------------

/// Trait for converting items into `Arc<dyn AsyncBehaviorNode<CTX>>`.
pub trait IntoAsyncBehaviorNode<CTX: Send + Sync + 'static> {
    fn into_async_behavior_node(self) -> Arc<dyn AsyncBehaviorNode<CTX>>;
}

/// `Arc<dyn AsyncBehaviorNode<CTX>>` → identity
impl<CTX: Send + Sync + 'static> IntoAsyncBehaviorNode<CTX> for Arc<dyn AsyncBehaviorNode<CTX>> {
    fn into_async_behavior_node(self) -> Arc<dyn AsyncBehaviorNode<CTX>> {
        self
    }
}

/// `Arc<T>` where `T: AsyncBehaviorNode<CTX>` → upcasted
impl<CTX, T> IntoAsyncBehaviorNode<CTX> for Arc<T>
where
    CTX: Send + Sync + 'static,
    T: AsyncBehaviorNode<CTX> + 'static,
{
    fn into_async_behavior_node(self) -> Arc<dyn AsyncBehaviorNode<CTX>> {
        self
    }
}

/// Bare async function `fn(AsyncExecutionContext<CTX>) -> impl Future` — auto-named
impl<CTX, F, Fut> IntoAsyncBehaviorNode<CTX> for F
where
    CTX: Send + Sync + 'static,
    F: Fn(AsyncExecutionContext<CTX>) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = ActionResult> + Send + 'static,
{
    fn into_async_behavior_node(self) -> Arc<dyn AsyncBehaviorNode<CTX>> {
        let type_name = std::any::type_name::<F>();
        let name = extract_function_name(type_name);
        Arc::new(FunctionNode::new(name, self))
    }
}

/// `(&'static str, F)` tuple — explicit name
impl<CTX, F, Fut> IntoAsyncBehaviorNode<CTX> for (&'static str, F)
where
    CTX: Send + Sync + 'static,
    F: Fn(AsyncExecutionContext<CTX>) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = ActionResult> + Send + 'static,
{
    fn into_async_behavior_node(self) -> Arc<dyn AsyncBehaviorNode<CTX>> {
        let (name, func) = self;
        Arc::new(FunctionNode::new(name, func))
    }
}

/// Extract function name from Rust type name
fn extract_function_name(type_name: &str) -> String {
    if type_name.contains("closure") {
        return "closure".to_string();
    }
    if type_name.starts_with("fn(") {
        return "function_pointer".to_string();
    }
    if let Some(last_part) = type_name.split("::").last() {
        if let Some(bracket_pos) = last_part.find('<') {
            last_part[..bracket_pos].to_string()
        } else {
            last_part.to_string()
        }
    } else {
        "function".to_string()
    }
}

fn make_action<CTX: Send + Sync + 'static>(node: impl IntoAsyncBehaviorNode<CTX>) -> BehaviorTreeNode<CTX> {
    BehaviorTreeNode::Action {
        id: NodeId::default(),
        node: node.into_async_behavior_node(),
    }
}

// ---------------------------------------------------------------------------
// Builders
// ---------------------------------------------------------------------------

/// Entry point for the fluent builder API.
pub struct BehaviorTreeBuilder;

/// Builder for Sequence nodes
pub struct SequenceBuilder<CTX> {
    name: String,
    children: Vec<BehaviorTreeNode<CTX>>,
}

/// Builder for Selector nodes
pub struct SelectorBuilder<CTX> {
    name: String,
    children: Vec<BehaviorTreeNode<CTX>>,
}

/// Builder for Parallel nodes
pub struct ParallelBuilder<CTX> {
    name: String,
    children: Vec<BehaviorTreeNode<CTX>>,
    policy: ParallelPolicy,
}

/// Builder for Condition nodes
pub struct ConditionBuilder<CTX> {
    name: String,
    condition: Arc<dyn Condition<CTX>>,
    true_branch: Option<BehaviorTreeNode<CTX>>,
    false_branch: Option<BehaviorTreeNode<CTX>>,
}

impl BehaviorTreeBuilder {
    pub fn new() -> Self {
        Self
    }

    pub fn action<CTX: Send + Sync + 'static>(action: impl IntoAsyncBehaviorNode<CTX>) -> BehaviorTreeNode<CTX> {
        make_action(action)
    }

    pub fn sequence<CTX: Send + Sync + 'static, S: Into<String>>(name: S) -> SequenceBuilder<CTX> {
        SequenceBuilder { name: name.into(), children: Vec::new() }
    }

    pub fn selector<CTX: Send + Sync + 'static, S: Into<String>>(name: S) -> SelectorBuilder<CTX> {
        SelectorBuilder { name: name.into(), children: Vec::new() }
    }

    pub fn parallel<CTX: Send + Sync + 'static, S: Into<String>>(name: S) -> ParallelBuilder<CTX> {
        ParallelBuilder {
            name: name.into(),
            children: Vec::new(),
            policy: ParallelPolicy::AllSucceed,
        }
    }

    pub fn condition<CTX: Send + Sync + 'static, S: Into<String>>(
        name: S,
        condition: Arc<dyn Condition<CTX>>,
    ) -> ConditionBuilder<CTX> {
        ConditionBuilder {
            name: name.into(),
            condition,
            true_branch: None,
            false_branch: None,
        }
    }
}

impl Default for BehaviorTreeBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl<CTX: Send + Sync + 'static> SequenceBuilder<CTX> {
    pub fn child(mut self, child: BehaviorTreeNode<CTX>) -> Self {
        self.children.push(child);
        self
    }

    pub fn action(mut self, action: impl IntoAsyncBehaviorNode<CTX>) -> Self {
        self.children.push(make_action(action));
        self
    }

    pub fn children(mut self, children: impl IntoIterator<Item = BehaviorTreeNode<CTX>>) -> Self {
        self.children.extend(children);
        self
    }

    pub fn build(self) -> BehaviorTreeNode<CTX> {
        BehaviorTreeNode::Sequence {
            id: NodeId::default(),
            name: self.name,
            children: self.children,
        }
    }
}

impl<CTX: Send + Sync + 'static> SelectorBuilder<CTX> {
    pub fn child(mut self, child: BehaviorTreeNode<CTX>) -> Self {
        self.children.push(child);
        self
    }

    pub fn action(mut self, action: impl IntoAsyncBehaviorNode<CTX>) -> Self {
        self.children.push(make_action(action));
        self
    }

    pub fn children(mut self, children: impl IntoIterator<Item = BehaviorTreeNode<CTX>>) -> Self {
        self.children.extend(children);
        self
    }

    pub fn build(self) -> BehaviorTreeNode<CTX> {
        BehaviorTreeNode::Selector {
            id: NodeId::default(),
            name: self.name,
            children: self.children,
        }
    }
}

impl<CTX: Send + Sync + 'static> ParallelBuilder<CTX> {
    pub fn policy(mut self, policy: ParallelPolicy) -> Self {
        self.policy = policy;
        self
    }

    pub fn all_succeed(mut self) -> Self {
        self.policy = ParallelPolicy::AllSucceed;
        self
    }

    pub fn first_succeed(mut self) -> Self {
        self.policy = ParallelPolicy::FirstSucceed;
        self
    }

    pub fn any_succeed(mut self) -> Self {
        self.policy = ParallelPolicy::AnySucceed;
        self
    }

    pub fn child(mut self, child: BehaviorTreeNode<CTX>) -> Self {
        self.children.push(child);
        self
    }

    pub fn action(mut self, action: impl IntoAsyncBehaviorNode<CTX>) -> Self {
        self.children.push(make_action(action));
        self
    }

    pub fn children(mut self, children: impl IntoIterator<Item = BehaviorTreeNode<CTX>>) -> Self {
        self.children.extend(children);
        self
    }

    pub fn build(self) -> BehaviorTreeNode<CTX> {
        BehaviorTreeNode::Parallel {
            id: NodeId::default(),
            name: self.name,
            children: self.children,
            policy: self.policy,
        }
    }
}

impl<CTX: Send + Sync + 'static> ConditionBuilder<CTX> {
    pub fn when_true(mut self, branch: BehaviorTreeNode<CTX>) -> Self {
        self.true_branch = Some(branch);
        self
    }

    pub fn when_false(mut self, branch: BehaviorTreeNode<CTX>) -> Self {
        self.false_branch = Some(branch);
        self
    }

    pub fn build(self) -> BehaviorTreeNode<CTX> {
        let true_branch = self
            .true_branch
            .expect("Condition node requires at least a true branch");
        BehaviorTreeNode::Condition {
            id: NodeId::default(),
            name: self.name,
            condition: self.condition,
            true_branch: Box::new(true_branch),
            false_branch: self.false_branch.map(Box::new),
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    #[allow(unused_imports)]
    use crate::blackboard::Blackboard;

    // Use () as CTX in all tests.

    #[derive(Debug)]
    struct MockAction {
        name: String,
    }

    #[async_trait::async_trait]
    impl AsyncBehaviorNode<()> for MockAction {
        async fn execute(&self, _ctx: AsyncExecutionContext<()>) -> ActionResult {
            ActionResult::Success
        }
        fn name(&self) -> &str { &self.name }
    }

    #[derive(Debug)]
    struct MockCondition {
        name: String,
        result: bool,
    }

    #[async_trait::async_trait]
    impl Condition<()> for MockCondition {
        async fn evaluate(&self, _ctx: &AsyncExecutionContext<()>) -> bool { self.result }
        fn name(&self) -> &str { &self.name }
    }

    async fn test_move_action(_ctx: AsyncExecutionContext<()>) -> ActionResult {
        ActionResult::Success
    }

    async fn test_scan_action(_ctx: AsyncExecutionContext<()>) -> ActionResult {
        ActionResult::Failure
    }

    #[test]
    fn test_action_builder_with_struct() {
        let action = Arc::new(MockAction { name: "test_action".to_string() });
        let node: BehaviorTreeNode<()> = BehaviorTreeBuilder::action(action);
        assert!(node.is_action());
        assert_eq!(node.name(), "test_action");
    }

    #[test]
    fn test_action_builder_with_function() {
        let node: BehaviorTreeNode<()> = BehaviorTreeBuilder::action(test_move_action);
        assert!(node.is_action());
        assert_eq!(node.name(), "test_move_action");
    }

    #[test]
    fn test_action_builder_with_named_function() {
        let node: BehaviorTreeNode<()> = BehaviorTreeBuilder::action(("custom_move", test_move_action));
        assert!(node.is_action());
        assert_eq!(node.name(), "custom_move");
    }

    #[test]
    fn test_sequence_builder_with_functions() {
        let node: BehaviorTreeNode<()> = BehaviorTreeBuilder::sequence("test_sequence")
            .action(test_move_action)
            .action(("scan", test_scan_action))
            .build();
        assert_eq!(node.name(), "test_sequence");
        assert_eq!(node.children().len(), 2);
        assert_eq!(node.children()[0].name(), "test_move_action");
        assert_eq!(node.children()[1].name(), "scan");
    }

    #[test]
    fn test_selector_builder_mixed() {
        let action1 = Arc::new(MockAction { name: "struct_action".to_string() });
        let node: BehaviorTreeNode<()> = BehaviorTreeBuilder::selector("test_selector")
            .action(action1)
            .action(test_move_action)
            .build();
        assert_eq!(node.name(), "test_selector");
        assert_eq!(node.children().len(), 2);
        assert_eq!(node.children()[0].name(), "struct_action");
        assert_eq!(node.children()[1].name(), "test_move_action");
    }

    #[test]
    fn test_parallel_builder_with_functions() {
        let node: BehaviorTreeNode<()> = BehaviorTreeBuilder::parallel("test_parallel")
            .first_succeed()
            .action(test_move_action)
            .action(test_scan_action)
            .build();
        assert_eq!(node.name(), "test_parallel");
        assert_eq!(node.children().len(), 2);
    }

    #[test]
    fn test_condition_builder() {
        let condition = Arc::new(MockCondition {
            name: "test_condition".to_string(),
            result: true,
        });
        let node: BehaviorTreeNode<()> = BehaviorTreeBuilder::condition("test_condition_node", condition)
            .when_true(BehaviorTreeBuilder::action(test_move_action))
            .build();
        assert_eq!(node.name(), "test_condition_node");
        assert_eq!(node.children().len(), 1);
    }

    #[test]
    fn test_function_name_extraction() {
        assert_eq!(
            extract_function_name("robot_bt_core::builder::tests::test_move_action"),
            "test_move_action"
        );
        assert_eq!(extract_function_name("closure"), "closure");
        assert_eq!(extract_function_name("fn() -> ActionResult"), "function_pointer");
        assert_eq!(extract_function_name("some_module::MyFunction<T>"), "MyFunction");
    }

    #[test]
    fn test_nested_tree_with_functions() {
        let condition = Arc::new(MockCondition {
            name: "battery_low".to_string(),
            result: false,
        });
        let tree: BehaviorTreeNode<()> = BehaviorTreeBuilder::selector("root")
            .child(
                BehaviorTreeBuilder::condition("battery_check", condition)
                    .when_true(BehaviorTreeBuilder::action(("charge", test_move_action)))
                    .build(),
            )
            .child(
                BehaviorTreeBuilder::parallel("patrol_parallel")
                    .all_succeed()
                    .action(test_move_action)
                    .action(test_scan_action)
                    .build(),
            )
            .build();
        assert_eq!(tree.name(), "root");
        assert_eq!(tree.children().len(), 2);
        assert_eq!(tree.children()[0].name(), "battery_check");
        assert_eq!(tree.children()[1].name(), "patrol_parallel");
        assert_eq!(tree.children()[1].children().len(), 2);
    }
}
