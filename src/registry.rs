//! `BtRegistry<CTX>` — a simple HashMap-based action and condition registry.
//!
//! The old `inventory`-based link-time registry is dropped because `inventory::collect!`
//! requires a concrete (non-generic) type. With `AsyncBehaviorNode<CTX>` being generic
//! over `CTX`, a link-time registry is not feasible.
//!
//! Instead, callers build a `BtRegistry<CTX>` and register factories explicitly before
//! calling `NodeDef::into_tree(&registry)`.

use std::collections::HashMap;
use std::sync::Arc;

use crate::condition::Condition;
use crate::node::AsyncBehaviorNode;

/// Factory function type for action nodes.
pub type ActionFactory<CTX> = fn() -> Arc<dyn AsyncBehaviorNode<CTX>>;

/// Factory function type for condition nodes.
pub type ConditionFactory<CTX> = fn() -> Arc<dyn Condition<CTX>>;

/// Runtime registry mapping node names to factory functions.
///
/// Create one per `CTX` type, register all your action and condition factories,
/// then pass `&registry` to `NodeDef::into_tree()`.
///
/// ```ignore
/// let mut reg = BtRegistry::<EngineContext>::new();
/// reg.register_action(|| Arc::new(MoveToTarget));
/// reg.register_condition(|| Arc::new(HasTarget));
/// let tree = NodeDef::from_yaml(yaml)?.into_tree(&reg)?;
/// ```
pub struct BtRegistry<CTX> {
    actions: HashMap<String, ActionFactory<CTX>>,
    conditions: HashMap<String, ConditionFactory<CTX>>,
}

impl<CTX: Send + Sync + 'static> BtRegistry<CTX> {
    /// Create an empty registry.
    pub fn new() -> Self {
        Self {
            actions: HashMap::new(),
            conditions: HashMap::new(),
        }
    }

    /// Register an action factory.
    ///
    /// The factory is called once per `into_tree()` for each matching node.
    /// The name key is derived from `factory().name()`.
    pub fn register_action(&mut self, factory: ActionFactory<CTX>) {
        let name = (factory)().name().to_string();
        self.actions.insert(name, factory);
    }

    /// Register a condition factory.
    ///
    /// The name key is derived from `factory().name()`.
    pub fn register_condition(&mut self, factory: ConditionFactory<CTX>) {
        let name = (factory)().name().to_string();
        self.conditions.insert(name, factory);
    }

    /// Resolve an action by name — returns `None` if not registered.
    pub fn resolve_action(&self, name: &str) -> Option<Arc<dyn AsyncBehaviorNode<CTX>>> {
        self.actions.get(name).map(|f| f())
    }

    /// Resolve a condition by name — returns `None` if not registered.
    pub fn resolve_condition(&self, name: &str) -> Option<Arc<dyn Condition<CTX>>> {
        self.conditions.get(name).map(|f| f())
    }

    /// All registered action names (sorted).
    pub fn action_names(&self) -> Vec<String> {
        let mut names: Vec<_> = self.actions.keys().cloned().collect();
        names.sort_unstable();
        names
    }

    /// All registered condition names (sorted).
    pub fn condition_names(&self) -> Vec<String> {
        let mut names: Vec<_> = self.conditions.keys().cloned().collect();
        names.sort_unstable();
        names
    }
}

impl<CTX: Send + Sync + 'static> Default for BtRegistry<CTX> {
    fn default() -> Self {
        Self::new()
    }
}
