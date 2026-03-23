//! Behavior Tree Structure Definition

use crate::types::ParallelPolicy;
use crate::{condition::Condition, node::AsyncBehaviorNode};
use std::hash::{Hash, Hasher};
use std::collections::hash_map::DefaultHasher;
use std::{fmt::Debug, sync::Arc};

/// Stable identifier for a node, stamped at tree construction time.
/// Stores a human-readable path string plus its pre-computed hash.
/// `Hash` is O(1) via pre-computed value; `Eq` checks hash first, then string on collision.
#[derive(Debug, Clone)]
pub struct NodeId {
    hash: u64,
    path: Arc<str>,
}

impl NodeId {
    pub fn root(name: &str) -> Self {
        Self::from_path(name)
    }

    pub fn child(parent: &NodeId, index: usize, name: &str) -> Self {
        let path = format!("{}/{}/{}", parent.path, index, name);
        Self::from_path(&path)
    }

    fn from_path(path: &str) -> Self {
        let mut h = DefaultHasher::new();
        path.hash(&mut h);
        NodeId { hash: h.finish(), path: Arc::from(path) }
    }

    pub fn path(&self) -> &str {
        &self.path
    }
}

impl Default for NodeId {
    /// Placeholder — replaced by `stamp_ids` before the tree runs.
    fn default() -> Self {
        Self::from_path("")
    }
}

impl PartialEq for NodeId {
    fn eq(&self, other: &Self) -> bool {
        self.hash == other.hash && self.path == other.path
    }
}

impl Eq for NodeId {}

impl Hash for NodeId {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.hash.hash(state);
    }
}

/// Behavior tree node definition
///
/// This enum represents the structure of the behavior tree.
/// The executor uses this to control traversal, not the nodes themselves.
#[derive(Debug, Clone)]
pub enum BehaviorTreeNode {
    /// Leaf action node - actual work is done here
    Action {
        id: NodeId,
        node: Arc<dyn AsyncBehaviorNode>,
    },

    /// Sequence - execute children in order until one fails
    Sequence {
        id: NodeId,
        name: String,
        children: Vec<BehaviorTreeNode>,
    },

    /// Selector - execute children until one succeeds
    Selector {
        id: NodeId,
        name: String,
        children: Vec<BehaviorTreeNode>,
    },

    /// Parallel - execute children concurrently
    Parallel {
        id: NodeId,
        name: String,
        children: Vec<BehaviorTreeNode>,
        policy: ParallelPolicy,
    },

    /// Condition - check condition and execute appropriate branch
    Condition {
        id: NodeId,
        name: String,
        condition: Arc<dyn Condition>,
        true_branch: Box<BehaviorTreeNode>,
        false_branch: Option<Box<BehaviorTreeNode>>,
    },
}

impl BehaviorTreeNode {
    /// Get this node's stable id
    pub fn id(&self) -> &NodeId {
        match self {
            BehaviorTreeNode::Action { id, .. } => id,
            BehaviorTreeNode::Sequence { id, .. } => id,
            BehaviorTreeNode::Selector { id, .. } => id,
            BehaviorTreeNode::Parallel { id, .. } => id,
            BehaviorTreeNode::Condition { id, .. } => id,
        }
    }

    /// Get the name of this node for debugging
    pub fn name(&self) -> &str {
        match self {
            BehaviorTreeNode::Action { node, .. } => node.name(),
            BehaviorTreeNode::Sequence { name, .. } => name,
            BehaviorTreeNode::Selector { name, .. } => name,
            BehaviorTreeNode::Parallel { name, .. } => name,
            BehaviorTreeNode::Condition { name, .. } => name,
        }
    }

    /// Stamp NodeIds on this node and all descendants.
    /// Call once after constructing the tree, before running.
    pub fn stamp_ids(&mut self, parent_id: Option<&NodeId>, index: usize) {
        let id = match parent_id {
            None => NodeId::root(self.name()),
            Some(parent) => NodeId::child(parent, index, self.name()),
        };

        match self {
            BehaviorTreeNode::Action { id: slot, .. } => *slot = id,
            BehaviorTreeNode::Sequence { id: slot, children, .. } => {
                *slot = id;
                for (i, child) in children.iter_mut().enumerate() {
                    child.stamp_ids(Some(slot), i);
                }
            }
            BehaviorTreeNode::Selector { id: slot, children, .. } => {
                *slot = id;
                for (i, child) in children.iter_mut().enumerate() {
                    child.stamp_ids(Some(slot), i);
                }
            }
            BehaviorTreeNode::Parallel { id: slot, children, .. } => {
                *slot = id;
                for (i, child) in children.iter_mut().enumerate() {
                    child.stamp_ids(Some(slot), i);
                }
            }
            BehaviorTreeNode::Condition { id: slot, true_branch, false_branch, .. } => {
                *slot = id;
                true_branch.stamp_ids(Some(slot), 0);
                if let Some(fb) = false_branch {
                    fb.stamp_ids(Some(slot), 1);
                }
            }
        }
    }

    /// Check if this is a leaf action node
    pub fn is_action(&self) -> bool {
        matches!(self, BehaviorTreeNode::Action { .. })
    }

    /// Get children nodes (if this is a composite node)
    pub fn children(&self) -> Vec<&BehaviorTreeNode> {
        match self {
            BehaviorTreeNode::Action { .. } => vec![],
            BehaviorTreeNode::Sequence { children, .. } => children.iter().collect(),
            BehaviorTreeNode::Selector { children, .. } => children.iter().collect(),
            BehaviorTreeNode::Parallel { children, .. } => children.iter().collect(),
            BehaviorTreeNode::Condition { true_branch, false_branch, .. } => {
                let mut result = vec![true_branch.as_ref()];
                if let Some(false_branch) = false_branch {
                    result.push(false_branch.as_ref());
                }
                result
            }
        }
    }
}