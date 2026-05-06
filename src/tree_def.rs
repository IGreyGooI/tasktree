//! Serializable behavior tree definition (`NodeDef`).
//!
//! `NodeDef` is a pure-data, serde-able mirror of `BehaviorTreeNode`.
//! Action and condition nodes are represented by their registered name strings;
//! resolution against the registry happens during `into_tree(&registry)`.
//!
//! ## Round-trip
//! ```text
//! YAML string  ──serde──▶  NodeDef  ──into_tree(&reg)──▶  BehaviorTreeNode<CTX>
//!                                                                 │
//!                                              BehaviorTreeRuntime::new(tree, bb, user)
//! ```

use serde::{Deserialize, Serialize};

use crate::{
    error::RobotBTError,
    registry::BtRegistry,
    tree::{BehaviorTreeNode, NodeId},
    types::ParallelPolicy,
};

/// Serializable behavior tree node definition.
///
/// Each variant mirrors `BehaviorTreeNode` but with action/condition nodes
/// represented as name strings (resolved via `BtRegistry`).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum NodeDef {
    /// Leaf action — resolved from the registry by name at load time.
    Action {
        /// Registered name (must match a registered factory).
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

    // ── XML helpers (xml feature) ─────────────────────────────────────────

    /// Deserialize from an XML string using the quick-xml event API.
    ///
    /// Element name is the node type; attributes carry scalar fields.
    /// `<true_branch>` / `<false_branch>` are wrapper elements for condition branches.
    ///
    /// ```xml
    /// <Selector name="root">
    ///   <Condition name="check" condition_name="player_present">
    ///     <true_branch><Action name="greet_player"/></true_branch>
    ///   </Condition>
    /// </Selector>
    /// ```
    #[cfg(feature = "xml")]
    pub fn from_xml(s: &str) -> Result<Self, RobotBTError> {
        xml_parser::parse_root(s)
    }

    // ── Conversion ───────────────────────────────────────────────────────

    /// Convert this definition into a runtime `BehaviorTreeNode<CTX>`.
    ///
    /// `registry` maps node names to factory functions. Returns `Err` if any
    /// action or condition name is not found in the registry.
    pub fn into_tree<CTX: Send + Sync + 'static>(
        self,
        registry: &BtRegistry<CTX>,
    ) -> Result<BehaviorTreeNode<CTX>, RobotBTError> {
        match self {
            NodeDef::Action { name } => {
                let node = registry.resolve_action(&name)
                    .ok_or_else(|| RobotBTError::UnknownAction { name: name.clone() })?;
                Ok(BehaviorTreeNode::Action {
                    id: NodeId::default(),
                    node,
                })
            }

            NodeDef::Sequence { name, children } => Ok(BehaviorTreeNode::Sequence {
                id: NodeId::default(),
                name,
                children: children
                    .into_iter()
                    .map(|c| c.into_tree(registry))
                    .collect::<Result<_, _>>()?,
            }),

            NodeDef::Selector { name, children } => Ok(BehaviorTreeNode::Selector {
                id: NodeId::default(),
                name,
                children: children
                    .into_iter()
                    .map(|c| c.into_tree(registry))
                    .collect::<Result<_, _>>()?,
            }),

            NodeDef::Parallel { name, policy, children } => Ok(BehaviorTreeNode::Parallel {
                id: NodeId::default(),
                name,
                policy,
                children: children
                    .into_iter()
                    .map(|c| c.into_tree(registry))
                    .collect::<Result<_, _>>()?,
            }),

            NodeDef::Condition { name, condition_name, true_branch, false_branch } => {
                let condition = registry.resolve_condition(&condition_name)
                    .ok_or_else(|| RobotBTError::UnknownCondition { name: condition_name })?;
                Ok(BehaviorTreeNode::Condition {
                    id: NodeId::default(),
                    name,
                    condition,
                    true_branch: Box::new(true_branch.into_tree(registry)?),
                    false_branch: false_branch
                        .map(|b| b.into_tree(registry).map(Box::new))
                        .transpose()?,
                })
            }
        }
    }
}

// ── XML parser internals (xml feature) ───────────────────────────────────────

#[cfg(feature = "xml")]
mod xml_parser;

// Re-export helpers into NodeDef::from_xml scope
#[cfg(feature = "xml")]

#[cfg(test)]
mod tests {
    use super::*;
    use crate::blackboard::Blackboard;
    use crate::registry::BtRegistry;
    use crate::types::{ActionResult, AsyncExecutionContext};
    use std::sync::Arc;

    // ── Minimal registered action & condition for tests ───────────────────

    #[derive(Debug)]
    struct NoopAction;

    #[async_trait::async_trait]
    impl crate::node::AsyncBehaviorNode<()> for NoopAction {
        async fn execute(&self, _ctx: AsyncExecutionContext<()>) -> ActionResult {
            ActionResult::Success
        }
        fn name(&self) -> &str { "noop" }
    }

    #[derive(Debug)]
    struct AlwaysTrue;

    #[async_trait::async_trait]
    impl crate::condition::Condition<()> for AlwaysTrue {
        async fn evaluate(&self, _ctx: &AsyncExecutionContext<()>) -> bool { true }
        fn name(&self) -> &str { "always_true" }
    }

    fn make_registry() -> BtRegistry<()> {
        let mut reg = BtRegistry::new();
        reg.register_action(|| Arc::new(NoopAction));
        reg.register_condition(|| Arc::new(AlwaysTrue));
        reg
    }

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
        let reg = make_registry();
        let node = NodeDef::Action { name: "noop".into() }.into_tree(&reg).unwrap();
        assert!(node.is_action());
        assert_eq!(node.name(), "noop");
    }

    #[test]
    fn into_tree_sequence() {
        let reg = make_registry();
        let def = NodeDef::Sequence {
            name: "seq".into(),
            children: vec![NodeDef::Action { name: "noop".into() }],
        };
        let node = def.into_tree(&reg).unwrap();
        assert_eq!(node.name(), "seq");
        assert_eq!(node.children().len(), 1);
    }

    #[test]
    fn into_tree_condition() {
        let reg = make_registry();
        let def = NodeDef::Condition {
            name: "cond".into(),
            condition_name: "always_true".into(),
            true_branch: Box::new(NodeDef::Action { name: "noop".into() }),
            false_branch: None,
        };
        let node = def.into_tree(&reg).unwrap();
        assert_eq!(node.name(), "cond");
    }

    #[test]
    fn into_tree_unknown_action_returns_err() {
        let reg = make_registry();
        let err = NodeDef::Action { name: "missing".into() }.into_tree(&reg).unwrap_err();
        assert!(matches!(err, crate::error::RobotBTError::UnknownAction { name } if name == "missing"));
    }

    #[test]
    fn into_tree_unknown_condition_returns_err() {
        let reg = make_registry();
        let err = NodeDef::Condition {
            name: "c".into(),
            condition_name: "missing".into(),
            true_branch: Box::new(NodeDef::Action { name: "noop".into() }),
            false_branch: None,
        }.into_tree(&reg).unwrap_err();
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
        let reg = make_registry();
        let mut rt = crate::runtime::BehaviorTreeRuntime::from_yaml(
            yaml,
            Blackboard::new(),
            Arc::new(()),
            &reg,
        ).unwrap();

        let result = rt.tick().await;
        assert_eq!(result, crate::types::NodeResult::Success);
    }

    // ── XML parse tests ───────────────────────────────────────────────────

    #[cfg(feature = "xml")]
    #[test]
    fn xml_parse_action_selfclosing() {
        let xml = r#"<Action name="noop"/>"#;
        let def = NodeDef::from_xml(xml).unwrap();
        assert!(matches!(def, NodeDef::Action { ref name } if name == "noop"));
    }

    #[cfg(feature = "xml")]
    #[test]
    fn xml_parse_sequence_two_actions() {
        let xml = r#"
<Sequence name="root">
  <Action name="noop"/>
  <Action name="noop"/>
</Sequence>"#;
        let def = NodeDef::from_xml(xml).unwrap();
        match def {
            NodeDef::Sequence { name, children } => {
                assert_eq!(name, "root");
                assert_eq!(children.len(), 2);
            }
            _ => panic!("expected Sequence"),
        }
    }

    #[cfg(feature = "xml")]
    #[test]
    fn xml_parse_selector() {
        let xml = r#"
<Selector name="sel">
  <Action name="noop"/>
</Selector>"#;
        let def = NodeDef::from_xml(xml).unwrap();
        assert!(matches!(def, NodeDef::Selector { .. }));
    }

    #[cfg(feature = "xml")]
    #[test]
    fn xml_parse_condition_true_branch_only() {
        let xml = r#"
<Condition name="check" condition_name="always_true">
  <true_branch>
    <Action name="noop"/>
  </true_branch>
</Condition>"#;
        let def = NodeDef::from_xml(xml).unwrap();
        match def {
            NodeDef::Condition { condition_name, false_branch, .. } => {
                assert_eq!(condition_name, "always_true");
                assert!(false_branch.is_none());
            }
            _ => panic!("expected Condition"),
        }
    }

    #[cfg(feature = "xml")]
    #[test]
    fn xml_parse_condition_both_branches() {
        let xml = r#"
<Condition name="check" condition_name="always_true">
  <true_branch>
    <Action name="noop"/>
  </true_branch>
  <false_branch>
    <Action name="noop"/>
  </false_branch>
</Condition>"#;
        let def = NodeDef::from_xml(xml).unwrap();
        match def {
            NodeDef::Condition { false_branch, .. } => assert!(false_branch.is_some()),
            _ => panic!("expected Condition"),
        }
    }

    #[cfg(feature = "xml")]
    #[test]
    fn xml_parse_branch_multi_children_becomes_sequence() {
        let xml = r#"
<Condition name="check" condition_name="always_true">
  <true_branch>
    <Action name="noop"/>
    <Action name="noop"/>
  </true_branch>
</Condition>"#;
        let def = NodeDef::from_xml(xml).unwrap();
        match def {
            NodeDef::Condition { true_branch, .. } => {
                assert!(
                    matches!(*true_branch, NodeDef::Sequence { .. }),
                    "expected implicit Sequence for multi-child branch, got {:?}",
                    true_branch
                );
            }
            _ => panic!("expected Condition"),
        }
    }

    #[cfg(feature = "xml")]
    #[tokio::test]
    async fn xml_runtime_from_xml_ticks() {
        let xml = r#"
<Sequence name="root">
  <Action name="noop"/>
  <Action name="noop"/>
</Sequence>"#;
        let reg = make_registry();
        let mut rt = crate::runtime::BehaviorTreeRuntime::from_xml(
            xml,
            Blackboard::new(),
            Arc::new(()),
            &reg,
        ).unwrap();
        let result = rt.tick().await;
        assert_eq!(result, crate::types::NodeResult::Success);
    }

    #[cfg(feature = "xml")]
    #[tokio::test]
    async fn xml_runtime_condition_routes_true_branch() {
        let xml = r#"
<Selector name="root">
  <Condition name="check" condition_name="always_true">
    <true_branch>
      <Action name="noop"/>
    </true_branch>
  </Condition>
</Selector>"#;
        let reg = make_registry();
        let mut rt = crate::runtime::BehaviorTreeRuntime::from_xml(
            xml,
            Blackboard::new(),
            Arc::new(()),
            &reg,
        ).unwrap();
        let result = rt.tick().await;
        assert_eq!(result, crate::types::NodeResult::Success);
    }

    // ── XML error-case tests ──────────────────────────────────────────────
    //
    // Each test asserts:
    //   1. `NodeDef::from_xml` returns `Err(RobotBTError::XmlError(...))`
    //   2. The inner `XmlError` is the expected typed variant
    //   3. (Some tests) `pos.line` / `pos.col` point at the right location
    //
    // These tests lock in both the error type AND the message. If either
    // changes, the test fails — intentional: errors are part of the
    // developer/LLM diagnostic interface.

    #[cfg(feature = "xml")]
    fn unwrap_xml_err(xml: &str) -> crate::error::XmlError {
        match NodeDef::from_xml(xml) {
            Ok(def) => panic!("expected XmlError but parse succeeded with: {def:?}"),
            Err(crate::error::RobotBTError::XmlError(xe)) => xe,
            Err(other) => panic!("expected XmlError variant, got: {other:?}"),
        }
    }

    #[cfg(feature = "xml")]
    #[test]
    fn xml_err_unknown_selfclosing_element_variant_and_name() {
        // Self-closing unknown element → InvalidSelfClosing (not UnknownElement),
        // because the parser rejects it before it can classify the type.
        let xe = unwrap_xml_err(r#"<Robot name="r1"/>"#);
        match xe {
            crate::error::XmlError::InvalidSelfClosing { name, pos } => {
                assert_eq!(name, "Robot");
                assert_eq!(pos.line, 1, "error should be on line 1");
            }
            other => panic!("expected InvalidSelfClosing, got: {other:?}"),
        }
    }

    #[cfg(feature = "xml")]
    #[test]
    fn xml_err_unknown_open_element_variant_and_name() {
        // Open unknown element → UnknownElement.
        let xe = unwrap_xml_err(r#"<Robot name="r1"><Action name="noop"/></Robot>"#);
        match xe {
            crate::error::XmlError::UnknownElement { name, pos } => {
                assert_eq!(name, "Robot");
                assert_eq!(pos.line, 1, "error should be on line 1");
            }
            other => panic!("expected UnknownElement, got: {other:?}"),
        }
    }

    #[cfg(feature = "xml")]
    #[test]
    fn xml_err_missing_name_attr_on_action() {
        let xe = unwrap_xml_err(r#"<Action/>"#);
        match xe {
            crate::error::XmlError::MissingAttribute { element, attr, .. } => {
                assert_eq!(element, "Action");
                assert_eq!(attr, "name");
            }
            other => panic!("expected MissingAttribute, got: {other:?}"),
        }
    }

    #[cfg(feature = "xml")]
    #[test]
    fn xml_err_missing_name_attr_on_sequence() {
        let xe = unwrap_xml_err(r#"<Sequence><Action name="noop"/></Sequence>"#);
        match xe {
            crate::error::XmlError::MissingAttribute { element, attr, .. } => {
                assert_eq!(element, "Sequence");
                assert_eq!(attr, "name");
            }
            other => panic!("expected MissingAttribute, got: {other:?}"),
        }
    }

    #[cfg(feature = "xml")]
    #[test]
    fn xml_err_missing_condition_name_attr() {
        let xe = unwrap_xml_err(
            r#"<Condition name="c"><true_branch><Action name="noop"/></true_branch></Condition>"#,
        );
        match xe {
            crate::error::XmlError::MissingAttribute { element, attr, .. } => {
                assert_eq!(element, "Condition");
                assert_eq!(attr, "condition_name");
            }
            other => panic!("expected MissingAttribute, got: {other:?}"),
        }
    }

    #[cfg(feature = "xml")]
    #[test]
    fn xml_err_condition_no_true_branch() {
        let xe = unwrap_xml_err(
            r#"<Condition name="c" condition_name="always_true">
                 <false_branch><Action name="noop"/></false_branch>
               </Condition>"#,
        );
        match xe {
            crate::error::XmlError::MissingTrueBranch { name, .. } => {
                assert_eq!(name, "c");
            }
            other => panic!("expected MissingTrueBranch, got: {other:?}"),
        }
    }

    #[cfg(feature = "xml")]
    #[test]
    fn xml_err_empty_branch_wrapper() {
        let xe = unwrap_xml_err(
            r#"<Condition name="c" condition_name="always_true">
                 <true_branch></true_branch>
               </Condition>"#,
        );
        assert!(
            matches!(xe, crate::error::XmlError::EmptyBranchWrapper { .. }),
            "expected EmptyBranchWrapper, got: {xe:?}"
        );
    }

    #[cfg(feature = "xml")]
    #[test]
    fn xml_err_wrong_closing_tag_caught_by_reader() {
        // quick-xml itself detects the mismatched close tag and raises a
        // ReaderError before our tracking code can fire UnexpectedClosingTag.
        let xe = unwrap_xml_err(r#"<Sequence name="root"><Action name="noop"/></Selector>"#);
        assert!(
            matches!(xe, crate::error::XmlError::ReaderError { .. }),
            "quick-xml should raise ReaderError for mismatched closing tag, got: {xe:?}"
        );
    }

    #[cfg(feature = "xml")]
    #[test]
    fn xml_err_selfclosing_non_action() {
        let xe = unwrap_xml_err(r#"<Sequence name="root"/>"#);
        match xe {
            crate::error::XmlError::InvalidSelfClosing { name, .. } => {
                assert_eq!(name, "Sequence");
            }
            other => panic!("expected InvalidSelfClosing, got: {other:?}"),
        }
    }

    #[cfg(feature = "xml")]
    #[test]
    fn xml_err_unknown_parallel_policy() {
        let xe = unwrap_xml_err(
            r#"<Parallel name="p" policy="AllFail"><Action name="noop"/></Parallel>"#,
        );
        match xe {
            crate::error::XmlError::UnknownPolicy { value, .. } => {
                assert_eq!(value, "AllFail");
            }
            other => panic!("expected UnknownPolicy, got: {other:?}"),
        }
    }

    /// Errors on line 2 should have `pos.line == 2`.
    #[cfg(feature = "xml")]
    #[test]
    fn xml_err_pos_line_number_is_tracked() {
        // Self-closing unknown element on line 2 → InvalidSelfClosing with pos.line == 2.
        let xml = "<Selector name=\"root\">\n  <Typo name=\"x\"/>\n</Selector>";
        let xe = unwrap_xml_err(xml);
        match xe {
            crate::error::XmlError::InvalidSelfClosing { name, pos } => {
                assert_eq!(name, "Typo");
                assert_eq!(pos.line, 2, "error should be on line 2, got: {pos:?}");
            }
            other => panic!("expected InvalidSelfClosing, got: {other:?}"),
        }
    }

    /// Unknown registry action: XML parse succeeds; into_tree() fails with UnknownAction.
    #[cfg(feature = "xml")]
    #[test]
    fn xml_err_unknown_action_in_registry() {
        let xml = r#"<Action name="does_not_exist"/>"#;
        let def = NodeDef::from_xml(xml).expect("valid XML — parse should succeed");
        let reg = make_registry();
        match def.into_tree(&reg) {
            Ok(_) => panic!("expected UnknownAction error"),
            Err(crate::error::RobotBTError::UnknownAction { name }) => {
                assert_eq!(name, "does_not_exist");
            }
            Err(other) => panic!("expected UnknownAction variant, got: {other:?}"),
        }
    }
}
