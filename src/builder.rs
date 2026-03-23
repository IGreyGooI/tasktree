//! Behavior Tree Builder - Fluent API for constructing behavior trees

use crate::{
    condition::Condition,
    node::AsyncBehaviorNode,
    tree::{BehaviorTreeNode, NodeId},
    types::{ActionResult, AsyncExecutionContext, ParallelPolicy},
};
use std::{any::type_name_of_val, future::Future};
use std::sync::Arc;

/// Trait for converting items into AsyncBehaviorNode
pub trait IntoAsyncBehaviorNode {
    fn into_async_behavior_node(self) -> Arc<dyn AsyncBehaviorNode>;
}

/// Wrapper for functions that implement AsyncBehaviorNode
pub struct FunctionNode<F> {
    name: String,
    func: F,
}

impl<F> FunctionNode<F> {
    pub fn new<S: Into<String>>(name: S, func: F) -> Self {
        Self {
            name: name.into(),
            func,
        }
    }
}

impl<F> std::fmt::Debug for FunctionNode<F> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FunctionNode")
            .field("name", &self.name)
            .field("func", &type_name_of_val(&self.func))
            .finish()
    }
}

#[async_trait::async_trait]
impl<F, Fut> AsyncBehaviorNode for FunctionNode<F>
where
    F: Fn(AsyncExecutionContext) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = ActionResult> + Send + 'static,
{
    async fn execute(&self, ctx: AsyncExecutionContext) -> ActionResult {
        (self.func)(ctx).await
    }

    fn name(&self) -> &str {
        &self.name
    }
}

// Implement IntoAsyncBehaviorNode for Arc<dyn AsyncBehaviorNode>
impl IntoAsyncBehaviorNode for Arc<dyn AsyncBehaviorNode> {
    fn into_async_behavior_node(self) -> Arc<dyn AsyncBehaviorNode> {
        self
    }
}

// Implement for any type that already implements AsyncBehaviorNode (via Arc)
impl<T> IntoAsyncBehaviorNode for Arc<T>
where
    T: AsyncBehaviorNode + 'static,
{
    fn into_async_behavior_node(self) -> Arc<dyn AsyncBehaviorNode> {
        self
    }
}

// Implement for functions with automatic name derivation
impl<F, Fut> IntoAsyncBehaviorNode for F
where
    F: Fn(AsyncExecutionContext) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = ActionResult> + Send + 'static,
{
    fn into_async_behavior_node(self) -> Arc<dyn AsyncBehaviorNode> {
        let type_name = std::any::type_name::<F>();
        let name = extract_function_name(type_name);
        Arc::new(FunctionNode::new(name, self))
    }
}

// Keep the tuple version for explicit naming
impl<F, Fut> IntoAsyncBehaviorNode for (&'static str, F)
where
    F: Fn(AsyncExecutionContext) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = ActionResult> + Send + 'static,
{
    fn into_async_behavior_node(self) -> Arc<dyn AsyncBehaviorNode> {
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

fn make_action(node: impl IntoAsyncBehaviorNode) -> BehaviorTreeNode {
    BehaviorTreeNode::Action {
        id: NodeId::default(),
        node: node.into_async_behavior_node(),
    }
}

/// Builder for constructing behavior trees with a fluent API
pub struct BehaviorTreeBuilder;

/// Builder for Sequence nodes
pub struct SequenceBuilder {
    name: String,
    children: Vec<BehaviorTreeNode>,
}

/// Builder for Selector nodes
pub struct SelectorBuilder {
    name: String,
    children: Vec<BehaviorTreeNode>,
}

/// Builder for Parallel nodes
pub struct ParallelBuilder {
    name: String,
    children: Vec<BehaviorTreeNode>,
    policy: ParallelPolicy,
}

/// Builder for Condition nodes
pub struct ConditionBuilder {
    name: String,
    condition: Arc<dyn Condition>,
    true_branch: Option<BehaviorTreeNode>,
    false_branch: Option<BehaviorTreeNode>,
}

impl BehaviorTreeBuilder {
    pub fn new() -> Self {
        Self
    }

    pub fn action(action: impl IntoAsyncBehaviorNode) -> BehaviorTreeNode {
        make_action(action)
    }

    pub fn sequence<S: Into<String>>(name: S) -> SequenceBuilder {
        SequenceBuilder { name: name.into(), children: Vec::new() }
    }

    pub fn selector<S: Into<String>>(name: S) -> SelectorBuilder {
        SelectorBuilder { name: name.into(), children: Vec::new() }
    }

    pub fn parallel<S: Into<String>>(name: S) -> ParallelBuilder {
        ParallelBuilder {
            name: name.into(),
            children: Vec::new(),
            policy: ParallelPolicy::AllSucceed,
        }
    }

    pub fn condition<S: Into<String>>(name: S, condition: Arc<dyn Condition>) -> ConditionBuilder {
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

impl SequenceBuilder {
    pub fn child(mut self, child: BehaviorTreeNode) -> Self {
        self.children.push(child);
        self
    }

    pub fn action(mut self, action: impl IntoAsyncBehaviorNode) -> Self {
        self.children.push(make_action(action));
        self
    }

    pub fn children(mut self, children: impl IntoIterator<Item = BehaviorTreeNode>) -> Self {
        self.children.extend(children);
        self
    }

    pub fn build(self) -> BehaviorTreeNode {
        BehaviorTreeNode::Sequence {
            id: NodeId::default(),
            name: self.name,
            children: self.children,
        }
    }
}

impl SelectorBuilder {
    pub fn child(mut self, child: BehaviorTreeNode) -> Self {
        self.children.push(child);
        self
    }

    pub fn action(mut self, action: impl IntoAsyncBehaviorNode) -> Self {
        self.children.push(make_action(action));
        self
    }

    pub fn children(mut self, children: impl IntoIterator<Item = BehaviorTreeNode>) -> Self {
        self.children.extend(children);
        self
    }

    pub fn build(self) -> BehaviorTreeNode {
        BehaviorTreeNode::Selector {
            id: NodeId::default(),
            name: self.name,
            children: self.children,
        }
    }
}

impl ParallelBuilder {
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

    pub fn child(mut self, child: BehaviorTreeNode) -> Self {
        self.children.push(child);
        self
    }

    pub fn action(mut self, action: impl IntoAsyncBehaviorNode) -> Self {
        self.children.push(make_action(action));
        self
    }

    pub fn children(mut self, children: impl IntoIterator<Item = BehaviorTreeNode>) -> Self {
        self.children.extend(children);
        self
    }

    pub fn build(self) -> BehaviorTreeNode {
        BehaviorTreeNode::Parallel {
            id: NodeId::default(),
            name: self.name,
            children: self.children,
            policy: self.policy,
        }
    }
}

impl ConditionBuilder {
    pub fn when_true(mut self, branch: BehaviorTreeNode) -> Self {
        self.true_branch = Some(branch);
        self
    }

    pub fn when_false(mut self, branch: BehaviorTreeNode) -> Self {
        self.false_branch = Some(branch);
        self
    }

    pub fn build(self) -> BehaviorTreeNode {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::blackboard::Blackboard;

    #[derive(Debug)]
    struct MockAction {
        name: String,
    }

    #[async_trait::async_trait]
    impl AsyncBehaviorNode for MockAction {
        async fn execute(&self, _ctx: AsyncExecutionContext) -> ActionResult {
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
    impl Condition for MockCondition {
        async fn evaluate(&self, _blackboard: &Blackboard) -> bool { self.result }
        fn name(&self) -> &str { &self.name }
    }

    async fn test_move_action(_ctx: AsyncExecutionContext) -> ActionResult {
        ActionResult::Success
    }

    async fn test_scan_action(_ctx: AsyncExecutionContext) -> ActionResult {
        ActionResult::Failure
    }

    #[test]
    fn test_action_builder_with_struct() {
        let action = Arc::new(MockAction { name: "test_action".to_string() });
        let node = BehaviorTreeBuilder::action(action);
        assert!(node.is_action());
        assert_eq!(node.name(), "test_action");
    }

    #[test]
    fn test_action_builder_with_function() {
        let node = BehaviorTreeBuilder::action(test_move_action);
        assert!(node.is_action());
        assert_eq!(node.name(), "test_move_action");
    }

    #[test]
    fn test_action_builder_with_named_function() {
        let node = BehaviorTreeBuilder::action(("custom_move", test_move_action));
        assert!(node.is_action());
        assert_eq!(node.name(), "custom_move");
    }

    #[test]
    fn test_sequence_builder_with_functions() {
        let node = BehaviorTreeBuilder::sequence("test_sequence")
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
        let node = BehaviorTreeBuilder::selector("test_selector")
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
        let node = BehaviorTreeBuilder::parallel("test_parallel")
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
        let node = BehaviorTreeBuilder::condition("test_condition_node", condition)
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
        let tree = BehaviorTreeBuilder::selector("root")
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