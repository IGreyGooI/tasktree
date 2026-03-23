//! Link-time action and condition registry via the `inventory` crate.
//!
//! Register actions and conditions from any crate with `register_action!` /
//! `register_condition!`. The runtime resolves names at tree-load time.

use std::sync::Arc;

use crate::{condition::Condition, node::AsyncBehaviorNode};

// ── Action registry ──────────────────────────────────────────────────────────

/// A registered action entry: a factory keyed by the node's own `name()`.
pub struct ActionRegistration {
    pub factory: fn() -> Arc<dyn AsyncBehaviorNode>,
}

inventory::collect!(ActionRegistration);

/// Register an action node type so it can be resolved from YAML by name.
///
/// The `name` in YAML must match what `AsyncBehaviorNode::name()` returns.
///
/// ```ignore
/// register_action!(|| Arc::new(MoveToTarget));
/// ```
#[macro_export]
macro_rules! register_action {
    ($factory:expr) => {
        $crate::inventory::submit!($crate::registry::ActionRegistration {
            factory: $factory,
        });
    };
}

// ── Condition registry ───────────────────────────────────────────────────────

/// A registered condition entry.
pub struct ConditionRegistration {
    pub factory: fn() -> Arc<dyn Condition>,
}

inventory::collect!(ConditionRegistration);

/// Register a condition type so it can be resolved from YAML by name.
///
/// The `name` in YAML must match what `Condition::name()` returns.
///
/// ```ignore
/// register_condition!(|| Arc::new(BatteryLow));
/// ```
#[macro_export]
macro_rules! register_condition {
    ($factory:expr) => {
        $crate::inventory::submit!($crate::registry::ConditionRegistration {
            factory: $factory,
        });
    };
}

// ── Manifest ─────────────────────────────────────────────────────────────────

/// All action names registered in the current binary.
pub fn registered_actions() -> Vec<String> {
    let mut names: Vec<String> = inventory::iter::<ActionRegistration>
        .into_iter()
        .map(|r| (r.factory)().name().to_string())
        .collect();
    names.sort_unstable();
    names
}

/// All condition names registered in the current binary.
pub fn registered_conditions() -> Vec<String> {
    let mut names: Vec<String> = inventory::iter::<ConditionRegistration>
        .into_iter()
        .map(|r| (r.factory)().name().to_string())
        .collect();
    names.sort_unstable();
    names
}

/// Resolve an action by name — calls the matching factory.
pub fn resolve_action(name: &str) -> Option<Arc<dyn AsyncBehaviorNode>> {
    inventory::iter::<ActionRegistration>
        .into_iter()
        .map(|r| (r.factory)())
        .find(|node| node.name() == name)
}

/// Resolve a condition by name — calls the matching factory.
pub fn resolve_condition(name: &str) -> Option<Arc<dyn Condition>> {
    inventory::iter::<ConditionRegistration>
        .into_iter()
        .map(|r| (r.factory)())
        .find(|cond| cond.name() == name)
}