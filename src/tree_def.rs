//! Serializable behavior tree definition (`NodeDef`).
//!
//! `NodeDef` is a pure-data, serde-able mirror of `BehaviorTreeNode`.
//! Action and condition nodes are represented by their registered name strings;
//! resolution against the registry happens during `into_tree()`.
//!
//! ## Round-trip
//! ```text
//! YAML string  ──serde──▶  NodeDef  ──into_tree()──▶  BehaviorTreeNode
//!                                                           │
//!                                              BehaviorTreeRuntime::new()
//! ```

use serde::{Deserialize, Serialize};

use crate::{
    error::RobotBTError,
    registry::{resolve_action, resolve_condition},
    tree::{BehaviorTreeNode, NodeId},
    types::ParallelPolicy,
};

/// Serializable behavior tree node definition.
///
/// Each variant mirrors `BehaviorTreeNode` but with action/condition nodes
/// represented as name strings (resolved via `registry`).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum NodeDef {
    /// Leaf action — resolved from the registry by name at load time.
    Action {
        /// Registered name (must match a `register_action!` call).
        name: String,
    },

    /// Execute children in order; stop on first failure.
    Sequence {
        name: String,
        children: Vec<NodeDef>,
    },

    /// Execute children in order; stop on first success.
    Selector {
        name: String,
        children: Vec<NodeDef>,
    },

    /// Execute all children concurrently under a policy.
    Parallel {
        name: String,
        #[serde(default)]
        policy: ParallelPolicy,
        children: Vec<NodeDef>,
    },

    /// Evaluate a condition (by name) and branch accordingly.
    Condition {
        name: String,
        /// Registered condition name.
        condition_name: String,
        true_branch: Box<NodeDef>,
        #[serde(default)]
        false_branch: Option<Box<NodeDef>>,
    },
}

impl NodeDef {
    // ── YAML helpers (serde feature) ─────────────────────────────────────

    /// Deserialize from a YAML string.
    #[cfg(feature = "serde")]
    pub fn from_yaml(s: &str) -> Result<Self, RobotBTError> {
        serde_norway::from_str(s).map_err(|e| RobotBTError::YamlError(e.to_string()))
    }

    /// Serialize to a YAML string.
    #[cfg(feature = "serde")]
    pub fn to_yaml(&self) -> Result<String, RobotBTError> {
        serde_norway::to_string(self).map_err(|e| RobotBTError::YamlError(e.to_string()))
    }

    // ── Conversion ───────────────────────────────────────────────────────

    /// Convert this definition into a runtime `BehaviorTreeNode`.
    ///
    /// Returns `Err` if any action or condition name is not found in the registry.
    pub fn into_tree(self) -> Result<BehaviorTreeNode, RobotBTError> {
        match self {
            NodeDef::Action { name } => {
                let node = resolve_action(&name)
                    .ok_or_else(|| RobotBTError::UnknownAction { name: name.clone() })?;
                Ok(BehaviorTreeNode::Action {
                    id: NodeId::default(),
                    node,
                })
            }

            NodeDef::Sequence { name, children } => Ok(BehaviorTreeNode::Sequence {
                id: NodeId::default(),
                name,
                children: children.into_iter().map(NodeDef::into_tree).collect::<Result<_, _>>()?,
            }),

            NodeDef::Selector { name, children } => Ok(BehaviorTreeNode::Selector {
                id: NodeId::default(),
                name,
                children: children.into_iter().map(NodeDef::into_tree).collect::<Result<_, _>>()?,
            }),

            NodeDef::Parallel { name, policy, children } => Ok(BehaviorTreeNode::Parallel {
                id: NodeId::default(),
                name,
                policy,
                children: children.into_iter().map(NodeDef::into_tree).collect::<Result<_, _>>()?,
            }),

            NodeDef::Condition { name, condition_name, true_branch, false_branch } => {
                let condition = resolve_condition(&condition_name)
                    .ok_or_else(|| RobotBTError::UnknownCondition { name: condition_name })?;
                Ok(BehaviorTreeNode::Condition {
                    id: NodeId::default(),
                    name,
                    condition,
                    true_branch: Box::new(true_branch.into_tree()?),
                    false_branch: false_branch.map(|b| b.into_tree().map(Box::new)).transpose()?,
                })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::blackboard::Blackboard;
    use crate::types::{ActionResult, AsyncExecutionContext};
    use std::sync::Arc;

    // ── Minimal registered action & condition for tests ───────────────────

    #[derive(Debug)]
    struct NoopAction;

    #[async_trait::async_trait]
    impl crate::node::AsyncBehaviorNode for NoopAction {
        async fn execute(&self, _ctx: AsyncExecutionContext) -> ActionResult {
            ActionResult::Success
        }
        fn name(&self) -> &str { "noop" }
    }

    #[derive(Debug)]
    struct AlwaysTrue;

    #[async_trait::async_trait]
    impl crate::condition::Condition for AlwaysTrue {
        async fn evaluate(&self, _bb: &Blackboard) -> bool { true }
        fn name(&self) -> &str { "always_true" }
    }

    // Register them once for the test binary via inventory
    inventory::submit!(crate::registry::ActionRegistration {
        factory: || Arc::new(NoopAction),
    });

    inventory::submit!(crate::registry::ConditionRegistration {
        factory: || Arc::new(AlwaysTrue),
    });

    // ── YAML parse tests ──────────────────────────────────────────────────

    #[test]
    fn parse_action() {
        let yaml = "type: Action\nname: noop\n";
        let def = NodeDef::from_yaml(yaml).unwrap();
        assert!(matches!(def, NodeDef::Action { name } if name == "noop"));
    }

    #[test]
    fn parse_sequence() {
        let yaml = "\
type: Sequence
name: root
children:
  - type: Action
    name: noop
  - type: Action
    name: noop
";
        let def = NodeDef::from_yaml(yaml).unwrap();
        match def {
            NodeDef::Sequence { name, children } => {
                assert_eq!(name, "root");
                assert_eq!(children.len(), 2);
            }
            _ => panic!("expected Sequence"),
        }
    }

    #[test]
    fn parse_selector() {
        let yaml = "\
type: Selector
name: sel
children:
  - type: Action
    name: noop
";
        let def = NodeDef::from_yaml(yaml).unwrap();
        assert!(matches!(def, NodeDef::Selector { .. }));
    }

    #[test]
    fn parse_parallel_default_policy() {
        let yaml = "\
type: Parallel
name: par
children:
  - type: Action
    name: noop
";
        let def = NodeDef::from_yaml(yaml).unwrap();
        match def {
            NodeDef::Parallel { policy, .. } => assert_eq!(policy, ParallelPolicy::AllSucceed),
            _ => panic!("expected Parallel"),
        }
    }

    #[test]
    fn parse_parallel_explicit_policy() {
        let yaml = "\
type: Parallel
name: par
policy: FirstSucceed
children:
  - type: Action
    name: noop
";
        let def = NodeDef::from_yaml(yaml).unwrap();
        match def {
            NodeDef::Parallel { policy, .. } => assert_eq!(policy, ParallelPolicy::FirstSucceed),
            _ => panic!("expected Parallel"),
        }
    }

    #[test]
    fn parse_condition_no_false_branch() {
        let yaml = "\
type: Condition
name: check
condition_name: always_true
true_branch:
  type: Action
  name: noop
";
        let def = NodeDef::from_yaml(yaml).unwrap();
        match def {
            NodeDef::Condition { condition_name, false_branch, .. } => {
                assert_eq!(condition_name, "always_true");
                assert!(false_branch.is_none());
            }
            _ => panic!("expected Condition"),
        }
    }

    #[test]
    fn parse_condition_with_false_branch() {
        let yaml = "\
type: Condition
name: check
condition_name: always_true
true_branch:
  type: Action
  name: noop
false_branch:
  type: Action
  name: noop
";
        let def = NodeDef::from_yaml(yaml).unwrap();
        match def {
            NodeDef::Condition { false_branch, .. } => assert!(false_branch.is_some()),
            _ => panic!("expected Condition"),
        }
    }

    // ── YAML round-trip ───────────────────────────────────────────────────

    #[test]
    fn round_trip_sequence() {
        let original = NodeDef::Sequence {
            name: "root".into(),
            children: vec![
                NodeDef::Action { name: "noop".into() },
                NodeDef::Action { name: "noop".into() },
            ],
        };
        let yaml = original.to_yaml().unwrap();
        let parsed = NodeDef::from_yaml(&yaml).unwrap();
        match parsed {
            NodeDef::Sequence { name, children } => {
                assert_eq!(name, "root");
                assert_eq!(children.len(), 2);
            }
            _ => panic!("expected Sequence"),
        }
    }

    // ── into_tree resolution ──────────────────────────────────────────────

    #[test]
    fn into_tree_action_resolved() {
        let node = NodeDef::Action { name: "noop".into() }.into_tree().unwrap();
        assert!(node.is_action());
        assert_eq!(node.name(), "noop");
    }

    #[test]
    fn into_tree_sequence() {
        let def = NodeDef::Sequence {
            name: "seq".into(),
            children: vec![NodeDef::Action { name: "noop".into() }],
        };
        let node = def.into_tree().unwrap();
        assert_eq!(node.name(), "seq");
        assert_eq!(node.children().len(), 1);
    }

    #[test]
    fn into_tree_condition() {
        let def = NodeDef::Condition {
            name: "cond".into(),
            condition_name: "always_true".into(),
            true_branch: Box::new(NodeDef::Action { name: "noop".into() }),
            false_branch: None,
        };
        let node = def.into_tree().unwrap();
        assert_eq!(node.name(), "cond");
    }

    #[test]
    fn into_tree_unknown_action_returns_err() {
        let err = NodeDef::Action { name: "missing".into() }.into_tree().unwrap_err();
        assert!(matches!(err, crate::error::RobotBTError::UnknownAction { name } if name == "missing"));
    }

    #[test]
    fn into_tree_unknown_condition_returns_err() {
        let err = NodeDef::Condition {
            name: "c".into(),
            condition_name: "missing".into(),
            true_branch: Box::new(NodeDef::Action { name: "noop".into() }),
            false_branch: None,
        }.into_tree().unwrap_err();
        assert!(matches!(err, crate::error::RobotBTError::UnknownCondition { name } if name == "missing"));
    }

    // ── BehaviorTreeRuntime::from_yaml ────────────────────────────────────

    #[tokio::test]
    async fn runtime_from_yaml_ticks() {
        let yaml = "\
type: Sequence
name: root
children:
  - type: Action
    name: noop
  - type: Action
    name: noop
";
        let mut rt = crate::runtime::BehaviorTreeRuntime::from_yaml(
            yaml,
            Blackboard::new(),
        ).unwrap();

        let result = rt.tick().await;
        assert_eq!(result, crate::types::NodeResult::Success);
    }
}