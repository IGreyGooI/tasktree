//! `BehaviorTreeRuntime` — the tick-driven async behavior tree executor.
//!
//! ## Design
//!
//! * The runtime owns `HashMap<NodeId, JoinHandle<ActionResult>>` for all
//!   in-flight action nodes.
//! * On `tick()` the tree is walked recursively via `eval()`.
//! * First encounter of an action node: `poll_once` probes the future once.
//!   - `Poll::Ready(r)` → completed synchronously, result this tick, no spawn.
//!   - `Poll::Pending`  → spawn, return `Running` to parent.
//! * Subsequent ticks with an existing handle: return `Running` (or collect the
//!   result if the handle is finished).
//! * `cancel_subtree` aborts all handles in the subtree (used by selector
//!   preemption).

use std::collections::HashMap;
use std::sync::Arc;

use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{debug, trace};

use crate::blackboard::Blackboard;
use crate::node::AsyncBehaviorNode;
use crate::poll::poll_once;
use crate::tree::{BehaviorTreeNode, NodeId};
use crate::types::{ActionResult, AsyncExecutionContext, NodeResult, ParallelPolicy};

/// Async behavior tree runtime.
///
/// Create one per tree; share the `Blackboard` across runtimes if multiple
/// trees need to communicate.
#[derive(Debug)]
pub struct BehaviorTreeRuntime {
    root: BehaviorTreeNode,
    blackboard: Blackboard,
    cancellation_token: CancellationToken,
    /// In-flight action handles, keyed by stable `NodeId`.
    handles: HashMap<NodeId, JoinHandle<ActionResult>>,
}

impl BehaviorTreeRuntime {
    pub fn new(mut root: BehaviorTreeNode, blackboard: Blackboard) -> Self {
        root.stamp_ids(None, 0);
        Self {
            root,
            blackboard,
            cancellation_token: CancellationToken::new(),
            handles: HashMap::new(),
        }
    }

    /// Deserialize a tree from a YAML string and create a runtime.
    ///
    /// Actions and conditions are resolved from the global registry
    /// (populated via `register_action!` / `register_condition!`).
    ///
    /// # Errors
    /// Returns a YAML parse error if the string is malformed.
    #[cfg(feature = "serde")]
    pub fn from_yaml(yaml: &str, blackboard: Blackboard) -> Result<Self, crate::error::RobotBTError> {
        let tree = crate::tree_def::NodeDef::from_yaml(yaml)?.into_tree()?;
        Ok(Self::new(tree, blackboard))
    }

    /// Reference to the shared blackboard.
    pub fn blackboard(&self) -> &Blackboard {
        &self.blackboard
    }

    /// Cancel the entire tree and abort all in-flight action futures.
    pub fn cancel(&mut self) {
        self.cancellation_token.cancel();
        for (_, handle) in self.handles.drain() {
            handle.abort();
        }
    }

    /// Execute one tick of the behavior tree.
    pub async fn tick(&mut self) -> NodeResult {
        let ctx = AsyncExecutionContext::new(self.blackboard.clone(), self.cancellation_token.clone());
        eval(&self.root, &mut self.handles, ctx).await
    }

    /// Abort all in-flight handles for `subtree` and its descendants.
    ///
    /// Called by selector nodes when a higher-priority child succeeds and the
    /// currently-running lower-priority subtree should be preempted.
    pub fn cancel_subtree(&mut self, subtree: &BehaviorTreeNode) {
        cancel_subtree_handles(subtree, &mut self.handles);
    }
}

// ---------------------------------------------------------------------------
// eval — recursive tree walker
// ---------------------------------------------------------------------------

fn eval<'a>(
    node: &'a BehaviorTreeNode,
    handles: &'a mut HashMap<NodeId, JoinHandle<ActionResult>>,
    ctx: AsyncExecutionContext,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = NodeResult> + Send + 'a>> {
    Box::pin(async move {
        if ctx.current_ct.is_cancelled() {
            return NodeResult::Failure;
        }

        match node {
            // ------------------------------------------------------------------
            // Action — poll once, spawn to tokio runtime if it does not return right away
            // ------------------------------------------------------------------
            BehaviorTreeNode::Action { id, node } => {
                eval_action(id.clone(), node.clone(), handles, ctx).await
            }

            // ------------------------------------------------------------------
            // Sequence — stop on first failure
            // ------------------------------------------------------------------
            BehaviorTreeNode::Sequence { name, children, .. } => {
                trace!("Sequence {}", name);
                for child in children {
                    match eval(child, handles, ctx.child_context()).await {
                        NodeResult::Success => continue,
                        other => return other,
                    }
                }
                NodeResult::Success
            }

            // ------------------------------------------------------------------
            // Selector — stop on first success
            // ------------------------------------------------------------------
            BehaviorTreeNode::Selector { name, children, .. } => {
                trace!("Selector {}", name);
                for child in children {
                    match eval(child, handles, ctx.child_context()).await {
                        NodeResult::Failure => {
                            // Clean up any handles the child left in-flight
                            // before trying the next child.
                            cancel_subtree_handles(child, handles);
                            continue;
                        }
                        other => return other,
                    }
                }
                NodeResult::Failure
            }

            // ------------------------------------------------------------------
            // Parallel — run all children; apply policy
            // ------------------------------------------------------------------
            BehaviorTreeNode::Parallel { name, children, policy, .. } => {
                trace!("Parallel {} ({:?})", name, policy);
                if children.is_empty() {
                    return NodeResult::Success;
                }
                let mut successes = 0usize;
                let mut failures = 0usize;
                let mut running = 0usize;
                for child in children {
                    match eval(child, handles, ctx.child_context()).await {
                        NodeResult::Success => successes += 1,
                        NodeResult::Failure => failures += 1,
                        NodeResult::Running => running += 1,
                    }
                }
                let result = apply_parallel_policy(*policy, successes, failures, running, children.len());
                // If policy triggered early exit (not still Running), cancel
                // all sibling handles that are still in-flight.
                if result != NodeResult::Running {
                    for child in children {
                        cancel_subtree_handles(child, handles);
                    }
                }
                result
            }

            // ------------------------------------------------------------------
            // Condition — evaluate predicate, execute branch
            // ------------------------------------------------------------------
            BehaviorTreeNode::Condition { name, condition, true_branch, false_branch, .. } => {
                trace!("Condition {}", name);
                if condition.evaluate(&ctx.blackboard).await {
                    eval(true_branch, handles, ctx).await
                } else if let Some(fb) = false_branch {
                    eval(fb, handles, ctx).await
                } else {
                    NodeResult::Failure
                }
            }
        }
    })
}

// ---------------------------------------------------------------------------
// eval_action — poll_once → spawn if Pending
// ---------------------------------------------------------------------------

async fn eval_action(
    id: NodeId,
    action: Arc<dyn AsyncBehaviorNode>,
    handles: &mut HashMap<NodeId, JoinHandle<ActionResult>>,
    ctx: AsyncExecutionContext,
) -> NodeResult {
    // Check for an existing in-flight handle.
    if let Some(handle) = handles.get(&id) {
        if handle.is_finished() {
            let handle = handles.remove(&id).unwrap();
            return match handle.await {
                Ok(r) => r.into(),
                Err(_) => NodeResult::Failure, // panicked or aborted
            };
        } else {
            debug!("Action '{}' still running", action.name());
            return NodeResult::Running;
        }
    }

    // No existing handle — probe once with a noop waker.
    // async_trait returns Pin<Box<dyn Future + Send + 'static>> so we can
    // probe it once and, if Pending, spawn the SAME future — execute() is
    // called exactly once regardless of outcome.
    let name = action.name().to_string();
    // Box the future with an explicit move of `action` so it is 'static.
    // poll_once probes it; if Pending we spawn the same boxed future.
    let spawn_ctx = ctx.child_context();
    let mut fut: std::pin::Pin<Box<dyn std::future::Future<Output = ActionResult> + Send + 'static>> =
        Box::pin(async move { action.execute(spawn_ctx).await });

    if let Some(result) = poll_once(fut.as_mut()) {
        // Completed synchronously — fast path, no spawn.
        trace!("Action '{}' resolved synchronously: {:?}", name, result);
        return result.into();
    }

    // Future returned Pending → spawn the already-probed future.
    let handle = tokio::spawn(fut);
    debug!("Action '{}' spawned (async)", name);
    handles.insert(id, handle);
    NodeResult::Running
}

// ---------------------------------------------------------------------------
// apply_parallel_policy
// ---------------------------------------------------------------------------

fn apply_parallel_policy(
    policy: ParallelPolicy,
    successes: usize,
    failures: usize,
    running: usize,
    total: usize,
) -> NodeResult {
    match policy {
        ParallelPolicy::AllSucceed => {
            if failures > 0 {
                NodeResult::Failure
            } else if successes == total {
                NodeResult::Success
            } else {
                NodeResult::Running
            }
        }
        ParallelPolicy::FirstSucceed => {
            if successes > 0 {
                NodeResult::Success
            } else if running == 0 {
                NodeResult::Failure
            } else {
                NodeResult::Running
            }
        }
        ParallelPolicy::AnySucceed => {
            if running > 0 {
                NodeResult::Running
            } else if successes > 0 {
                NodeResult::Success
            } else {
                NodeResult::Failure
            }
        }
    }
}

// ---------------------------------------------------------------------------
// cancel_subtree_handles
// ---------------------------------------------------------------------------

fn cancel_subtree_handles(
    node: &BehaviorTreeNode,
    handles: &mut HashMap<NodeId, JoinHandle<ActionResult>>,
) {
    if let Some(h) = handles.remove(node.id()) {
        h.abort();
    }
    for child in node.children() {
        cancel_subtree_handles(child, handles);
    }
    if let BehaviorTreeNode::Condition { true_branch, false_branch, .. } = node {
        cancel_subtree_handles(true_branch, handles);
        if let Some(fb) = false_branch {
            cancel_subtree_handles(fb, handles);
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::blackboard::Blackboard;
    use crate::node::AsyncBehaviorNode;
    use crate::tree::BehaviorTreeNode;
    use crate::types::{ActionResult, AsyncExecutionContext, NodeResult};
    use async_trait::async_trait;
    use std::sync::Arc;

    // --- helpers ---

    fn action_node(node: impl AsyncBehaviorNode + 'static) -> BehaviorTreeNode {
        BehaviorTreeNode::Action {
            id: NodeId::root("placeholder"), // overwritten by BehaviorTreeRuntime::new
            node: Arc::new(node),
        }
    }

    fn rt(root: BehaviorTreeNode) -> BehaviorTreeRuntime {
        BehaviorTreeRuntime::new(root, Blackboard::new())
    }

    // --- mock nodes ---

    #[derive(Debug)]
    struct AlwaysSuccess;

    #[async_trait]
    impl AsyncBehaviorNode for AlwaysSuccess {
        async fn execute(&self, _ctx: AsyncExecutionContext) -> ActionResult {
            ActionResult::Success
        }
        fn name(&self) -> &str { "AlwaysSuccess" }
    }

    #[derive(Debug)]
    struct AlwaysFailure;

    #[async_trait]
    impl AsyncBehaviorNode for AlwaysFailure {
        async fn execute(&self, _ctx: AsyncExecutionContext) -> ActionResult {
            ActionResult::Failure
        }
        fn name(&self) -> &str { "AlwaysFailure" }
    }

    /// Completes after a short async delay — forces the spawn path.
    #[derive(Debug)]
    struct DelayedSuccess;

    #[async_trait]
    impl AsyncBehaviorNode for DelayedSuccess {
        async fn execute(&self, _ctx: AsyncExecutionContext) -> ActionResult {
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            ActionResult::Success
        }
        fn name(&self) -> &str { "DelayedSuccess" }
    }

    #[derive(Debug)]
    struct DelayedFailure;

    #[async_trait]
    impl AsyncBehaviorNode for DelayedFailure {
        async fn execute(&self, _ctx: AsyncExecutionContext) -> ActionResult {
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            ActionResult::Failure
        }
        fn name(&self) -> &str { "DelayedFailure" }
    }

    /// Counts how many times execute() was called.
    #[derive(Debug)]
    struct CountedSuccess(Arc<std::sync::atomic::AtomicUsize>);

    #[async_trait]
    impl AsyncBehaviorNode for CountedSuccess {
        async fn execute(&self, _ctx: AsyncExecutionContext) -> ActionResult {
            self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            ActionResult::Success
        }
        fn name(&self) -> &str { "CountedSuccess" }
    }

    // --- tests ---

    #[tokio::test]
    async fn sync_action_completes_in_one_tick() {
        // AlwaysSuccess has no .await — poll_once should catch it immediately.
        let mut rt = rt(action_node(AlwaysSuccess));
        assert_eq!(rt.tick().await, NodeResult::Success);
        // No handles should remain.
        assert!(rt.handles.is_empty());
    }

    /// Sync action: poll_once catches it — execute() called exactly once, no spawn.
    #[tokio::test]
    async fn sync_action_execute_called_exactly_once() {
        let counter = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let mut rt = rt(action_node(CountedSuccess(counter.clone())));

        let result = rt.tick().await;

        assert_eq!(result, NodeResult::Success);
        assert_eq!(counter.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert!(rt.handles.is_empty()); // never spawned
    }

    #[tokio::test]
    async fn sync_failure_in_one_tick() {
        let mut rt = rt(action_node(AlwaysFailure));
        assert_eq!(rt.tick().await, NodeResult::Failure);
        assert!(rt.handles.is_empty());
    }

    #[tokio::test]
    async fn async_action_running_then_success() {
        // DelayedSuccess hits .await → spawned → Running on tick 1.
        let mut rt = rt(action_node(DelayedSuccess));

        let r1 = rt.tick().await;
        assert_eq!(r1, NodeResult::Running);
        assert_eq!(rt.handles.len(), 1);

        // Wait for the action to finish.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let r2 = rt.tick().await;
        assert_eq!(r2, NodeResult::Success);
        assert!(rt.handles.is_empty());
    }

    #[tokio::test]
    async fn sequence_all_succeed() {
        let root = BehaviorTreeNode::Sequence {
            id: NodeId::root("seq"),
            name: "seq".into(),
            children: vec![action_node(AlwaysSuccess), action_node(AlwaysSuccess)],
        };
        let mut rt = BehaviorTreeRuntime::new(root, Blackboard::new());
        assert_eq!(rt.tick().await, NodeResult::Success);
    }

    #[tokio::test]
    async fn sequence_stops_on_failure() {
        let root = BehaviorTreeNode::Sequence {
            id: NodeId::root("seq"),
            name: "seq".into(),
            children: vec![action_node(AlwaysFailure), action_node(AlwaysSuccess)],
        };
        let mut rt = BehaviorTreeRuntime::new(root, Blackboard::new());
        assert_eq!(rt.tick().await, NodeResult::Failure);
    }

    #[tokio::test]
    async fn selector_first_success_wins() {
        let root = BehaviorTreeNode::Selector {
            id: NodeId::root("sel"),
            name: "sel".into(),
            children: vec![action_node(AlwaysSuccess), action_node(AlwaysFailure)],
        };
        let mut rt = BehaviorTreeRuntime::new(root, Blackboard::new());
        assert_eq!(rt.tick().await, NodeResult::Success);
    }

    #[tokio::test]
    async fn selector_falls_through_to_success() {
        let root = BehaviorTreeNode::Selector {
            id: NodeId::root("sel"),
            name: "sel".into(),
            children: vec![action_node(AlwaysFailure), action_node(AlwaysSuccess)],
        };
        let mut rt = BehaviorTreeRuntime::new(root, Blackboard::new());
        assert_eq!(rt.tick().await, NodeResult::Success);
    }

    #[tokio::test]
    async fn selector_all_fail() {
        let root = BehaviorTreeNode::Selector {
            id: NodeId::root("sel"),
            name: "sel".into(),
            children: vec![action_node(AlwaysFailure), action_node(AlwaysFailure)],
        };
        let mut rt = BehaviorTreeRuntime::new(root, Blackboard::new());
        assert_eq!(rt.tick().await, NodeResult::Failure);
    }

    /// Selector: child[0] is long-running → selector returns Running, child[1] never touched.
    /// When child[0] eventually succeeds → selector returns Success.
    #[tokio::test]
    async fn selector_waits_for_running_child_then_succeeds() {
        let root = BehaviorTreeNode::Selector {
            id: NodeId::root("sel"),
            name: "sel".into(),
            children: vec![
                action_node(DelayedSuccess), // child[0]: long-running
                action_node(AlwaysSuccess),  // child[1]: never reached while child[0] runs
            ],
        };
        let mut rt = BehaviorTreeRuntime::new(root, Blackboard::new());

        // Tick 1: child[0] spawned → Running. child[1] not evaluated.
        assert_eq!(rt.tick().await, NodeResult::Running);
        assert_eq!(rt.handles.len(), 1); // only child[0] in-flight

        // Wait for child[0] to finish.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // Tick 2: child[0] done with Success → selector returns Success immediately.
        assert_eq!(rt.tick().await, NodeResult::Success);
        assert!(rt.handles.is_empty());
    }

    /// Selector: child[0] is long-running but eventually fails →
    /// selector falls through to child[1] which succeeds.
    #[tokio::test]
    async fn selector_long_running_failure_falls_through() {
        let root = BehaviorTreeNode::Selector {
            id: NodeId::root("sel"),
            name: "sel".into(),
            children: vec![
                action_node(DelayedFailure), // child[0]: long-running, fails
                action_node(AlwaysSuccess),  // child[1]: fallback
            ],
        };
        let mut rt = BehaviorTreeRuntime::new(root, Blackboard::new());

        // Tick 1: child[0] spawned → Running.
        assert_eq!(rt.tick().await, NodeResult::Running);
        assert_eq!(rt.handles.len(), 1);

        // Wait for child[0] to finish.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // Tick 2: child[0] done with Failure → try child[1] → Success.
        assert_eq!(rt.tick().await, NodeResult::Success);
        assert!(rt.handles.is_empty());
    }

    /// Parallel AllSucceed: one short, one long — Running until both done.
    #[tokio::test]
    async fn parallel_all_succeed_waits_for_slow_child() {
        let root = BehaviorTreeNode::Parallel {
            id: NodeId::root("par"),
            name: "par".into(),
            policy: crate::types::ParallelPolicy::AllSucceed,
            children: vec![
                action_node(AlwaysSuccess),  // child[0]: instant
                action_node(DelayedSuccess), // child[1]: slow
            ],
        };
        let mut rt = BehaviorTreeRuntime::new(root, Blackboard::new());

        // Tick 1: child[0] succeeds immediately, child[1] spawned → Running.
        assert_eq!(rt.tick().await, NodeResult::Running);
        assert_eq!(rt.handles.len(), 1); // only child[1] in-flight

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // Tick 2: child[0] re-evals as instant success, child[1] done → AllSucceed.
        assert_eq!(rt.tick().await, NodeResult::Success);
        assert!(rt.handles.is_empty());
    }

    /// Parallel AllSucceed: any failure → overall Failure immediately.
    #[tokio::test]
    async fn parallel_all_succeed_fails_on_any_failure() {
        let root = BehaviorTreeNode::Parallel {
            id: NodeId::root("par"),
            name: "par".into(),
            policy: crate::types::ParallelPolicy::AllSucceed,
            children: vec![
                action_node(AlwaysFailure),  // child[0]: instant fail
                action_node(DelayedSuccess), // child[1]: slow, shouldn't matter
            ],
        };
        let mut rt = BehaviorTreeRuntime::new(root, Blackboard::new());

        // child[0] fails immediately → AllSucceed returns Failure.
        assert_eq!(rt.tick().await, NodeResult::Failure);
    }

    /// Parallel FirstSucceed: first to succeed wins, even if others are running.
    #[tokio::test]
    async fn parallel_first_succeed_short_wins() {
        let root = BehaviorTreeNode::Parallel {
            id: NodeId::root("par"),
            name: "par".into(),
            policy: crate::types::ParallelPolicy::FirstSucceed,
            children: vec![
                action_node(DelayedSuccess), // child[0]: slow
                action_node(AlwaysSuccess),  // child[1]: instant
            ],
        };
        let mut rt = BehaviorTreeRuntime::new(root, Blackboard::new());

        // Tick 1: child[0] spawned, child[1] succeeds instantly → FirstSucceed wins.
        assert_eq!(rt.tick().await, NodeResult::Success);
    }

    /// Parallel AnySucceed: waits for all, succeeds if at least one succeeded.
    #[tokio::test]
    async fn parallel_any_succeed_waits_for_all() {
        let root = BehaviorTreeNode::Parallel {
            id: NodeId::root("par"),
            name: "par".into(),
            policy: crate::types::ParallelPolicy::AnySucceed,
            children: vec![
                action_node(AlwaysSuccess),  // child[0]: instant success
                action_node(DelayedFailure), // child[1]: slow failure
            ],
        };
        let mut rt = BehaviorTreeRuntime::new(root, Blackboard::new());

        // Tick 1: child[0] done, child[1] running → AnySucceed waits for all.
        assert_eq!(rt.tick().await, NodeResult::Running);

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // Tick 2: both done, one succeeded → AnySucceed returns Success.
        assert_eq!(rt.tick().await, NodeResult::Success);
        assert!(rt.handles.is_empty());
    }

    /// Regression: when a parallel inside a selector fails (one child fails fast,
    /// sibling still running), the sibling's handle must be cancelled — not leaked.
    #[tokio::test]
    async fn selector_cancels_leaked_handles_from_failed_parallel() {
        let root = BehaviorTreeNode::Selector {
            id: NodeId::root("sel"),
            name: "sel".into(),
            children: vec![
                BehaviorTreeNode::Parallel {
                    id: NodeId::root("par"),
                    name: "par".into(),
                    policy: crate::types::ParallelPolicy::AllSucceed,
                    children: vec![
                        action_node(AlwaysFailure),  // fails immediately → parallel fails
                        action_node(DelayedSuccess), // still running → should be cancelled
                    ],
                },
                action_node(AlwaysSuccess), // selector fallback
            ],
        };
        let mut rt = BehaviorTreeRuntime::new(root, Blackboard::new());

        // Parallel fails → selector falls to child[1] → Success.
        // DelayedSuccess handle must NOT leak.
        let result = rt.tick().await;
        assert_eq!(result, NodeResult::Success);
        assert!(rt.handles.is_empty(), "leaked handles: {}", rt.handles.len());
    }

    /// Parallel AllSucceed: one child fails immediately — sibling's in-flight
    /// handle must be cancelled, not leaked.
    #[tokio::test]
    async fn parallel_all_succeed_cancels_sibling_on_failure() {
        let root = BehaviorTreeNode::Parallel {
            id: NodeId::root("par"),
            name: "par".into(),
            policy: crate::types::ParallelPolicy::AllSucceed,
            children: vec![
                action_node(AlwaysFailure),  // fails immediately → AllSucceed fails
                action_node(DelayedSuccess), // still running → must be cancelled
            ],
        };
        let mut rt = BehaviorTreeRuntime::new(root, Blackboard::new());

        assert_eq!(rt.tick().await, NodeResult::Failure);
        assert!(rt.handles.is_empty(), "leaked handles: {}", rt.handles.len());
    }

    /// Parallel FirstSucceed: one child succeeds immediately — sibling's in-flight
    /// handle must be cancelled, not leaked.
    #[tokio::test]
    async fn parallel_first_succeed_cancels_sibling_on_success() {
        let root = BehaviorTreeNode::Parallel {
            id: NodeId::root("par"),
            name: "par".into(),
            policy: crate::types::ParallelPolicy::FirstSucceed,
            children: vec![
                action_node(DelayedSuccess), // slow → spawned
                action_node(AlwaysSuccess),  // succeeds immediately → FirstSucceed wins
            ],
        };
        let mut rt = BehaviorTreeRuntime::new(root, Blackboard::new());

        assert_eq!(rt.tick().await, NodeResult::Success);
        assert!(rt.handles.is_empty(), "leaked handles: {}", rt.handles.len());
    }

    /// rt.cancel() must abort all in-flight handles.
    #[tokio::test]
    async fn cancel_clears_all_handles() {
        let root = BehaviorTreeNode::Parallel {
            id: NodeId::root("par"),
            name: "par".into(),
            policy: crate::types::ParallelPolicy::AllSucceed,
            children: vec![
                action_node(DelayedSuccess),
                action_node(DelayedSuccess),
                action_node(DelayedSuccess),
            ],
        };
        let mut rt = BehaviorTreeRuntime::new(root, Blackboard::new());

        assert_eq!(rt.tick().await, NodeResult::Running);
        assert_eq!(rt.handles.len(), 3);

        rt.cancel();
        assert!(rt.handles.is_empty(), "leaked handles after cancel: {}", rt.handles.len());
    }

    /// Deeply nested: selector → parallel(AllSucceed) → parallel(AllSucceed)
    /// Inner parallel fails fast, outer parallel fails, selector falls through.
    /// No handles should leak at any level.
    #[tokio::test]
    async fn deeply_nested_failure_no_leaked_handles() {
        let root = BehaviorTreeNode::Selector {
            id: NodeId::root("sel"),
            name: "sel".into(),
            children: vec![
                BehaviorTreeNode::Parallel {
                    id: NodeId::root("outer_par"),
                    name: "outer_par".into(),
                    policy: crate::types::ParallelPolicy::AllSucceed,
                    children: vec![
                        BehaviorTreeNode::Parallel {
                            id: NodeId::root("inner_par"),
                            name: "inner_par".into(),
                            policy: crate::types::ParallelPolicy::AllSucceed,
                            children: vec![
                                action_node(AlwaysFailure),  // fails → inner fails
                                action_node(DelayedSuccess), // sibling → must cancel
                            ],
                        },
                        action_node(DelayedSuccess), // outer sibling → must cancel
                    ],
                },
                action_node(AlwaysSuccess), // fallback
            ],
        };
        let mut rt = BehaviorTreeRuntime::new(root, Blackboard::new());

        assert_eq!(rt.tick().await, NodeResult::Success);
        assert!(rt.handles.is_empty(), "leaked handles: {}", rt.handles.len());
    }

    /// Sequence: middle child is a parallel that eventually fails.
    /// No handles should accumulate across ticks.
    #[tokio::test]
    async fn sequence_parallel_failure_no_leaked_handles() {
        let root = BehaviorTreeNode::Sequence {
            id: NodeId::root("seq"),
            name: "seq".into(),
            children: vec![
                action_node(AlwaysSuccess),
                BehaviorTreeNode::Parallel {
                    id: NodeId::root("par"),
                    name: "par".into(),
                    policy: crate::types::ParallelPolicy::AllSucceed,
                    children: vec![
                        action_node(DelayedFailure), // slow fail
                        action_node(DelayedSuccess), // slow success
                    ],
                },
                action_node(AlwaysSuccess),
            ],
        };
        let mut rt = BehaviorTreeRuntime::new(root, Blackboard::new());

        // Tick 1: parallel running (2 handles)
        assert_eq!(rt.tick().await, NodeResult::Running);
        assert_eq!(rt.handles.len(), 2);

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // Tick 2: DelayedFailure done → AllSucceed fails → cancel DelayedSuccess
        // Sequence fails → no handles remain
        assert_eq!(rt.tick().await, NodeResult::Failure);
        assert!(rt.handles.is_empty(), "leaked handles: {}", rt.handles.len());
    }

    /// Re-ticking a running node does not spawn duplicate handles.
    #[tokio::test]
    async fn no_duplicate_handles_across_ticks() {
        let root = BehaviorTreeNode::Parallel {
            id: NodeId::root("par"),
            name: "par".into(),
            policy: crate::types::ParallelPolicy::AllSucceed,
            children: vec![
                action_node(DelayedSuccess),
                action_node(DelayedSuccess),
            ],
        };
        let mut rt = BehaviorTreeRuntime::new(root, Blackboard::new());

        // Multiple ticks while running — handle count must stay at 2, not grow
        for _ in 0..5 {
            assert_eq!(rt.tick().await, NodeResult::Running);
            assert_eq!(rt.handles.len(), 2, "handles grew unexpectedly");
        }

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert_eq!(rt.tick().await, NodeResult::Success);
        assert!(rt.handles.is_empty());
    }

    /// Action that watches a blackboard key and completes when it changes.
    #[cfg(feature = "watch")]
    #[derive(Debug)]
    struct WatchAction {
        key: &'static str,
    }

    #[cfg(feature = "watch")]
    #[async_trait]
    impl AsyncBehaviorNode for WatchAction {
        fn name(&self) -> &str { "WatchAction" }
        async fn execute(&self, ctx: AsyncExecutionContext) -> ActionResult {
            let mut rx = ctx.blackboard.watch(self.key).await;
            rx.changed().await.unwrap();
            ActionResult::Success
        }
    }

    #[cfg(feature = "watch")]
    #[tokio::test]
    async fn watch_action_wakes_on_blackboard_write() {
        use crate::blackboard::BlackboardKey;
        crate::define_key!(SIGNAL: i32 = "test/signal");

        let bb = Blackboard::new();
        let mut rt = BehaviorTreeRuntime::new(
            action_node(WatchAction { key: SIGNAL::KEY }),
            bb.clone(),
        );

        // Tick 1: action spawned, waiting on watch → Running.
        assert_eq!(rt.tick().await, NodeResult::Running);
        assert_eq!(rt.handles.len(), 1);

        // Write to the key — this fires the watch channel and wakes the action.
        bb.insert_key::<SIGNAL>(1).await;

        // Give the spawned task a moment to process the wakeup.
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;

        // Tick 2: action completed → Success.
        assert_eq!(rt.tick().await, NodeResult::Success);
        assert!(rt.handles.is_empty());
    }

    /// Selector: child[0] succeeds immediately — child[1] (long-running) never starts.
    #[tokio::test]
    async fn selector_short_success_skips_long_child() {
        let root = BehaviorTreeNode::Selector {
            id: NodeId::root("sel"),
            name: "sel".into(),
            children: vec![
                action_node(AlwaysSuccess),  // child[0]: instant success
                action_node(DelayedSuccess), // child[1]: never touched
            ],
        };
        let mut rt = BehaviorTreeRuntime::new(root, Blackboard::new());

        assert_eq!(rt.tick().await, NodeResult::Success);
        assert!(rt.handles.is_empty()); // child[1] was never spawned
    }
}
