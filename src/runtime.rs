//! `BehaviorTreeRuntime<CTX>` — tick-driven async behavior tree executor.
//!
//! # Quick start
//!
//! ```rust,ignore
//! use std::sync::Arc;
//! use tasktree::{BehaviorTreeRuntime, Blackboard};
//!
//! // 1. Build a tree (or load YAML — see `from_yaml`).
//! let tree = /* your BehaviorTreeNode<MyCtx> */;
//!
//! // 2. Create the runtime with a shared user context.
//! let mut rt = BehaviorTreeRuntime::new(tree, Blackboard::new(), Arc::new(my_ctx));
//!
//! // 3. Drive it. One tick = one walk of the tree.
//! loop {
//!     match rt.tick().await {
//!         NodeResult::Success | NodeResult::Failure => break,
//!         NodeResult::Running => { /* sleep, then tick again */ }
//!     }
//! }
//!
//! // 4. Pre-empt mid-tree (e.g. external event changes the situation):
//! rt.interrupt();           // cancels in-flight work, refreshes the token
//! rt.tick().await;          // walks from root again
//! ```
//!
//! # Writing an action node
//!
//! Implement [`AsyncBehaviorNode<CTX>`].  The single rule that matters:
//!
//! > **Do NOT `tokio::spawn` work and return `Success` early.**  Anything you
//! > spawn outside the action future cannot be tracked by the runtime, cannot
//! > be cancelled, and will keep running after the BT has moved on.  Just
//! > `.await` your work inside `execute` — the runtime spawns the future for
//! > you and stores the handle.
//!
//! ```rust,ignore
//! #[async_trait]
//! impl AsyncBehaviorNode<MyCtx> for SpeakToPlayer {
//!     async fn execute(&self, ctx: AsyncExecutionContext<MyCtx>) -> ActionResult {
//!         // Long async work — `.await` directly.  The runtime owns this future.
//!         let dialogue = match call_llm(&ctx.user, &ctx.current_ct).await {
//!             Ok(d) => d,
//!             Err(_) => return ActionResult::Failure,
//!         };
//!         emit_event(&ctx.user, dialogue).await;
//!         ActionResult::Success
//!     }
//!     fn name(&self) -> &str { "speak_to_player" }
//! }
//! ```
//!
//! ## Cancellation
//!
//! **Primary mechanism: `JoinHandle::abort()`.**  When `interrupt()` or
//! `cancel()` is called, every in-flight action handle is aborted.  Tokio
//! drops the future at its next `.await` point — no cooperation needed.
//! Any action that is structured as `do_work().await; ActionResult::Success`
//! is cancelled automatically because the future is dropped mid-stream.
//!
//! **`ctx.current_ct`** (a `CancellationToken`) serves two narrower roles:
//!
//! 1. **Tree traversal gate** — `eval()` checks `ctx.current_ct.is_cancelled()`
//!    before evaluating each node.  After `cancel()` (where the token is
//!    permanently fired), every subsequent `tick()` returns `Failure`
//!    immediately without walking any children.
//! 2. **Retry-loop fast-path** — if an action has a retry loop that calls a
//!    fallible async helper, checking `ct.is_cancelled()` before each attempt
//!    avoids starting new work after cancellation has been requested.
//!
//! Action nodes do **not** need to observe `ctx.current_ct` inside their main
//! async body for cancellation to work.  `abort()` handles mid-work
//! termination; checking the token is only useful for early-exit at the
//! boundary of retry attempts.
//!
//! **Cleanup on cancel** — resource cleanup (e.g. sending a "stream closed"
//! message to the frontend) must use a `Drop` impl on a local guard struct,
//! NOT a `select!` on the token.  `Drop` runs on both normal return and
//! `abort()`, so it is the only reliable cleanup hook.
//!
//! # Lifecycle
//!
//! * **Tick 1** — the runtime polls the action's future once.  If it returns
//!   `Ready` immediately, that result is the action's result for the tick (no
//!   spawn).  If it returns `Pending`, the runtime spawns it and stores the
//!   handle keyed by `NodeId`; the action's parent sees `Running`.
//! * **Tick N** — if the handle is still alive, the runtime returns `Running`
//!   without re-entering `execute`. Composite nodes with memory semantics keep
//!   resuming the same running child/branch on later ticks until it reaches a
//!   terminal result or [`Self::interrupt`] resets the tree.
//! * **Completion** — the handle reports `Ready(ActionResult)`; the runtime
//!   removes it from the map and bubbles the result up.
//!
//! [`Self::cancel`] permanently cancels the runtime.  [`Self::interrupt`] is
//! the reusable variant — it aborts in-flight handles, fires the cancellation
//! token, and installs a fresh token so the next `tick` runs cleanly.
//!
//! # CTX
//!
//! `CTX` is your engine context (world handle, NPC id, anything action nodes
//! need).  It is stored as `Arc<CTX>` and cloned cheaply into every
//! `AsyncExecutionContext`.  `CTX: Send + Sync + 'static`.

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
/// Create one per tree. Share the `Blackboard` across runtimes if multiple
/// trees need to communicate.
///
/// `CTX` is the engine-specific user context type.
#[derive(Debug)]
pub struct BehaviorTreeRuntime<CTX> {
    root: BehaviorTreeNode<CTX>,
    blackboard: Blackboard,
    user: Arc<CTX>,
    cancellation_token: CancellationToken,
    /// In-flight action handles, keyed by stable `NodeId`.
    handles: HashMap<NodeId, JoinHandle<ActionResult>>,
    /// Memory-style "currently running child/branch" pointers for composites.
    active_children: HashMap<NodeId, usize>,
}

impl<CTX: Send + Sync + 'static> BehaviorTreeRuntime<CTX> {
    /// Create a new runtime from a pre-built tree.
    ///
    /// `user` is the engine-specific context shared across all action nodes.
    pub fn new(mut root: BehaviorTreeNode<CTX>, blackboard: Blackboard, user: Arc<CTX>) -> Self {
        root.stamp_ids(None, 0);
        Self {
            root,
            blackboard,
            user,
            cancellation_token: CancellationToken::new(),
            handles: HashMap::new(),
            active_children: HashMap::new(),
        }
    }

    /// Deserialize a tree from a YAML string and create a runtime.
    ///
    /// Actions and conditions are resolved from `registry`.
    ///
    /// # Errors
    /// Returns a YAML parse error if the string is malformed, or an unknown-action
    /// error if the YAML references a name not in `registry`.
    #[cfg(feature = "serde")]
    pub fn from_yaml(
        yaml: &str,
        blackboard: Blackboard,
        user: Arc<CTX>,
        registry: &crate::registry::BtRegistry<CTX>,
    ) -> Result<Self, crate::error::RobotBTError> {
        let tree = crate::tree_def::NodeDef::from_yaml(yaml)?.into_tree(registry)?;
        Ok(Self::new(tree, blackboard, user))
    }

    /// Deserialize a tree from an XML string and create a runtime.
    ///
    /// Element name is the node type; attributes carry scalar fields.
    /// `<true_branch>` / `<false_branch>` are wrapper elements for `Condition` branches.
    /// Multiple children inside a branch wrapper are implicitly wrapped in a `Sequence`.
    ///
    /// # Errors
    /// Returns an XML parse error if the string is malformed, or an unknown-action
    /// error if the XML references a name not in `registry`.
    #[cfg(feature = "xml")]
    pub fn from_xml(
        xml: &str,
        blackboard: Blackboard,
        user: Arc<CTX>,
        registry: &crate::registry::BtRegistry<CTX>,
    ) -> Result<Self, crate::error::RobotBTError> {
        let tree = crate::tree_def::NodeDef::from_xml(xml)?.into_tree(registry)?;
        Ok(Self::new(tree, blackboard, user))
    }

    /// Reference to the shared blackboard.
    pub fn blackboard(&self) -> &Blackboard {
        &self.blackboard
    }

    /// Cancel the entire tree and abort all in-flight action futures.
    ///
    /// After `cancel()` the runtime's cancellation token is permanently cancelled.
    /// Call `interrupt()` instead if you want to resume ticking from the root.
    pub fn cancel(&mut self) {
        self.cancellation_token.cancel();
        for (_, handle) in self.handles.drain() {
            handle.abort();
        }
        self.active_children.clear();
    }

    /// Pre-empt the running tree so the next `tick()` walks fresh from the root.
    ///
    /// Aborts every in-flight action handle, fires the runtime's cancellation
    /// token (so cooperative cancellation reaches anything inside actions that
    /// is observing `ctx.current_ct`), then installs a fresh token.
    ///
    /// Use this when an external event changes the situation and the BT must
    /// re-decide from the top — e.g. the player speaks to an NPC mid-roam.
    /// In contrast, [`Self::cancel`] cancels permanently; subsequent ticks
    /// would see the cancelled token and return `Failure` immediately.
    pub fn interrupt(&mut self) {
        // Cancel BT structural handles and replace the cancellation token.
        self.cancel();
        self.cancellation_token = CancellationToken::new();
    }

    /// Execute one tick of the behavior tree.
    ///
    /// Walks the tree from the root, evaluating conditions and polling
    /// in-flight action handles.  The `user` context is cloned (via `Arc`)
    /// into the `AsyncExecutionContext` for this tick so action nodes and
    /// conditions can read engine state.
    pub async fn tick(&mut self) -> NodeResult {
        let ctx = AsyncExecutionContext::new(
            self.blackboard.clone(),
            self.cancellation_token.clone(),
            Arc::clone(&self.user),
        );
        eval(&self.root, &mut self.handles, &mut self.active_children, ctx).await
    }

    /// Abort all in-flight handles for `subtree` and its descendants.
    ///
    /// Called by selector nodes when a higher-priority child succeeds and the
    /// currently-running lower-priority subtree should be preempted.
    pub fn cancel_subtree(&mut self, subtree: &BehaviorTreeNode<CTX>) {
        cancel_subtree_state(subtree, &mut self.handles, &mut self.active_children);
    }
}

// ---------------------------------------------------------------------------
// eval — recursive tree walker
// ---------------------------------------------------------------------------

fn eval<'a, CTX: Send + Sync + 'static>(
    node: &'a BehaviorTreeNode<CTX>,
    handles: &'a mut HashMap<NodeId, JoinHandle<ActionResult>>,
    active_children: &'a mut HashMap<NodeId, usize>,
    ctx: AsyncExecutionContext<CTX>,
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
            BehaviorTreeNode::Sequence { id, name, children, .. } => {
                trace!("Sequence {}", name);
                let start_index = active_children.get(id).copied().unwrap_or(0);
                for (index, child) in children.iter().enumerate().skip(start_index) {
                    match eval(child, handles, active_children, ctx.child_context()).await {
                        NodeResult::Success => {
                            active_children.remove(id);
                            continue;
                        }
                        NodeResult::Failure => {
                            active_children.remove(id);
                            cancel_subtree_state(child, handles, active_children);
                            return NodeResult::Failure;
                        }
                        NodeResult::Running => {
                            active_children.insert(id.clone(), index);
                            return NodeResult::Running;
                        }
                    }
                }
                active_children.remove(id);
                NodeResult::Success
            }

            // ------------------------------------------------------------------
            // Selector — stop on first success
            // ------------------------------------------------------------------
            BehaviorTreeNode::Selector { id, name, children, .. } => {
                trace!("Selector {}", name);
                let start_index = active_children.get(id).copied().unwrap_or(0);
                for (index, child) in children.iter().enumerate().skip(start_index) {
                    match eval(child, handles, active_children, ctx.child_context()).await {
                        NodeResult::Failure => {
                            active_children.remove(id);
                            // Clean up any handles the child left in-flight
                            // before trying the next child.
                            cancel_subtree_state(child, handles, active_children);
                            continue;
                        }
                        NodeResult::Success => {
                            active_children.remove(id);
                            return NodeResult::Success;
                        }
                        NodeResult::Running => {
                            active_children.insert(id.clone(), index);
                            return NodeResult::Running;
                        }
                    }
                }
                active_children.remove(id);
                NodeResult::Failure
            }

            // ------------------------------------------------------------------
            // Parallel — run all children; apply policy
            // ------------------------------------------------------------------
            BehaviorTreeNode::Parallel {
                name,
                children,
                policy,
                ..
            } => {
                trace!("Parallel {} ({:?})", name, policy);
                if children.is_empty() {
                    return NodeResult::Success;
                }
                let mut successes = 0usize;
                let mut failures = 0usize;
                let mut running = 0usize;
                for child in children {
                    match eval(child, handles, active_children, ctx.child_context()).await {
                        NodeResult::Success => successes += 1,
                        NodeResult::Failure => failures += 1,
                        NodeResult::Running => running += 1,
                    }
                }
                let result =
                    apply_parallel_policy(*policy, successes, failures, running, children.len());
                // If policy triggered early exit (not still Running), cancel
                // all sibling handles that are still in-flight.
                if result != NodeResult::Running {
                    for child in children {
                        cancel_subtree_state(child, handles, active_children);
                    }
                }
                result
            }

            // ------------------------------------------------------------------
            // Condition — evaluate predicate, execute branch
            // ------------------------------------------------------------------
            BehaviorTreeNode::Condition {
                id,
                name,
                condition,
                true_branch,
                false_branch,
                ..
            } => {
                trace!("Condition {}", name);
                if let Some(active_branch) = active_children.get(id).copied() {
                    let result = eval_condition_branch(
                        id,
                        active_branch,
                        true_branch,
                        false_branch.as_deref(),
                        handles,
                        active_children,
                        ctx,
                    )
                    .await;
                    if result != NodeResult::Running {
                        active_children.remove(id);
                    }
                    return result;
                }

                let selected_branch = if condition.evaluate(&ctx).await {
                    Some(0)
                } else if false_branch.is_some() {
                    Some(1)
                } else {
                    None
                };

                let Some(selected_branch) = selected_branch else {
                    active_children.remove(id);
                    return NodeResult::Failure;
                };

                let result = eval_condition_branch(
                    id,
                    selected_branch,
                    true_branch,
                    false_branch.as_deref(),
                    handles,
                    active_children,
                    ctx,
                )
                .await;
                if result != NodeResult::Running {
                    active_children.remove(id);
                }
                result
            }
        }
    })
}

// ---------------------------------------------------------------------------
// eval_condition_branch
// ---------------------------------------------------------------------------

async fn eval_condition_branch<CTX: Send + Sync + 'static>(
    id: &NodeId,
    branch_index: usize,
    true_branch: &BehaviorTreeNode<CTX>,
    false_branch: Option<&BehaviorTreeNode<CTX>>,
    handles: &mut HashMap<NodeId, JoinHandle<ActionResult>>,
    active_children: &mut HashMap<NodeId, usize>,
    ctx: AsyncExecutionContext<CTX>,
) -> NodeResult {
    let branch = match branch_index {
        0 => true_branch,
        1 => match false_branch {
            Some(branch) => branch,
            None => {
                active_children.remove(id);
                return NodeResult::Failure;
            }
        },
        _ => {
            active_children.remove(id);
            return NodeResult::Failure;
        }
    };

    match eval(branch, handles, active_children, ctx).await {
        NodeResult::Running => {
            active_children.insert(id.clone(), branch_index);
            NodeResult::Running
        }
        other => other,
    }
}

// ---------------------------------------------------------------------------
// eval_action — poll_once → spawn if Pending
// ---------------------------------------------------------------------------

async fn eval_action<CTX: Send + Sync + 'static>(
    id: NodeId,
    action: Arc<dyn AsyncBehaviorNode<CTX>>,
    handles: &mut HashMap<NodeId, JoinHandle<ActionResult>>,
    ctx: AsyncExecutionContext<CTX>,
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
    let name = action.name().to_string();
    let spawn_ctx = ctx.child_context();
    let mut fut: std::pin::Pin<
        Box<dyn std::future::Future<Output = ActionResult> + Send + 'static>,
    > = Box::pin(async move { action.execute(spawn_ctx).await });

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
// cancel_subtree_state
// ---------------------------------------------------------------------------

fn cancel_subtree_state<CTX: Send + Sync + 'static>(
    node: &BehaviorTreeNode<CTX>,
    handles: &mut HashMap<NodeId, JoinHandle<ActionResult>>,
    active_children: &mut HashMap<NodeId, usize>,
) {
    active_children.remove(node.id());
    if let Some(h) = handles.remove(node.id()) {
        h.abort();
    }
    for child in node.children() {
        cancel_subtree_state(child, handles, active_children);
    }
    if let BehaviorTreeNode::Condition {
        true_branch,
        false_branch,
        ..
    } = node
    {
        cancel_subtree_state(true_branch, handles, active_children);
        if let Some(fb) = false_branch {
            cancel_subtree_state(fb, handles, active_children);
        }
    }
}

// ---------------------------------------------------------------------------
// Tests — CTX = ()
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::blackboard::Blackboard;
    use crate::condition::Condition;
    use crate::node::AsyncBehaviorNode;
    use crate::tree::BehaviorTreeNode;
    use crate::types::{ActionResult, AsyncExecutionContext, NodeResult};
    use async_trait::async_trait;
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };

    // --- helpers ---

    fn action_node(node: impl AsyncBehaviorNode<()> + 'static) -> BehaviorTreeNode<()> {
        BehaviorTreeNode::Action {
            id: NodeId::root("placeholder"), // overwritten by BehaviorTreeRuntime::new
            node: Arc::new(node),
        }
    }

    fn rt(root: BehaviorTreeNode<()>) -> BehaviorTreeRuntime<()> {
        BehaviorTreeRuntime::new(root, Blackboard::new(), Arc::new(()))
    }

    // --- mock nodes ---

    #[derive(Debug)]
    struct AlwaysSuccess;

    #[async_trait]
    impl AsyncBehaviorNode<()> for AlwaysSuccess {
        async fn execute(&self, _ctx: AsyncExecutionContext<()>) -> ActionResult {
            ActionResult::Success
        }
        fn name(&self) -> &str {
            "AlwaysSuccess"
        }
    }

    #[derive(Debug)]
    struct AlwaysFailure;

    #[async_trait]
    impl AsyncBehaviorNode<()> for AlwaysFailure {
        async fn execute(&self, _ctx: AsyncExecutionContext<()>) -> ActionResult {
            ActionResult::Failure
        }
        fn name(&self) -> &str {
            "AlwaysFailure"
        }
    }

    /// Completes after a short async delay — forces the spawn path.
    #[derive(Debug)]
    struct DelayedSuccess;

    #[async_trait]
    impl AsyncBehaviorNode<()> for DelayedSuccess {
        async fn execute(&self, _ctx: AsyncExecutionContext<()>) -> ActionResult {
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            ActionResult::Success
        }
        fn name(&self) -> &str {
            "DelayedSuccess"
        }
    }

    #[derive(Debug)]
    struct DelayedFailure;

    #[async_trait]
    impl AsyncBehaviorNode<()> for DelayedFailure {
        async fn execute(&self, _ctx: AsyncExecutionContext<()>) -> ActionResult {
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            ActionResult::Failure
        }
        fn name(&self) -> &str {
            "DelayedFailure"
        }
    }

    /// Counts how many times execute() was called.
    #[derive(Debug)]
    struct CountedSuccess(Arc<std::sync::atomic::AtomicUsize>);

    #[async_trait]
    impl AsyncBehaviorNode<()> for CountedSuccess {
        async fn execute(&self, _ctx: AsyncExecutionContext<()>) -> ActionResult {
            self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            ActionResult::Success
        }
        fn name(&self) -> &str {
            "CountedSuccess"
        }
    }

    #[derive(Debug)]
    struct FlagCondition(Arc<AtomicBool>);

    #[async_trait]
    impl Condition<()> for FlagCondition {
        async fn evaluate(&self, _ctx: &AsyncExecutionContext<()>) -> bool {
            self.0.load(Ordering::SeqCst)
        }

        fn name(&self) -> &str {
            "FlagCondition"
        }
    }

    // --- tests ---

    #[tokio::test]
    async fn sync_action_completes_in_one_tick() {
        let mut rt = rt(action_node(AlwaysSuccess));
        assert_eq!(rt.tick().await, NodeResult::Success);
        assert!(rt.handles.is_empty());
    }

    #[tokio::test]
    async fn sync_action_execute_called_exactly_once() {
        let counter = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let mut rt = rt(action_node(CountedSuccess(counter.clone())));

        let result = rt.tick().await;

        assert_eq!(result, NodeResult::Success);
        assert_eq!(counter.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert!(rt.handles.is_empty());
    }

    #[tokio::test]
    async fn sync_failure_in_one_tick() {
        let mut rt = rt(action_node(AlwaysFailure));
        assert_eq!(rt.tick().await, NodeResult::Failure);
        assert!(rt.handles.is_empty());
    }

    #[tokio::test]
    async fn async_action_running_then_success() {
        let mut rt = rt(action_node(DelayedSuccess));

        let r1 = rt.tick().await;
        assert_eq!(r1, NodeResult::Running);
        assert_eq!(rt.handles.len(), 1);

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
        let mut rt = BehaviorTreeRuntime::new(root, Blackboard::new(), Arc::new(()));
        assert_eq!(rt.tick().await, NodeResult::Success);
    }

    #[tokio::test]
    async fn sequence_stops_on_failure() {
        let root = BehaviorTreeNode::Sequence {
            id: NodeId::root("seq"),
            name: "seq".into(),
            children: vec![action_node(AlwaysFailure), action_node(AlwaysSuccess)],
        };
        let mut rt = BehaviorTreeRuntime::new(root, Blackboard::new(), Arc::new(()));
        assert_eq!(rt.tick().await, NodeResult::Failure);
    }

    #[tokio::test]
    async fn selector_first_success_wins() {
        let root = BehaviorTreeNode::Selector {
            id: NodeId::root("sel"),
            name: "sel".into(),
            children: vec![action_node(AlwaysSuccess), action_node(AlwaysFailure)],
        };
        let mut rt = BehaviorTreeRuntime::new(root, Blackboard::new(), Arc::new(()));
        assert_eq!(rt.tick().await, NodeResult::Success);
    }

    #[tokio::test]
    async fn selector_falls_through_to_success() {
        let root = BehaviorTreeNode::Selector {
            id: NodeId::root("sel"),
            name: "sel".into(),
            children: vec![action_node(AlwaysFailure), action_node(AlwaysSuccess)],
        };
        let mut rt = BehaviorTreeRuntime::new(root, Blackboard::new(), Arc::new(()));
        assert_eq!(rt.tick().await, NodeResult::Success);
    }

    #[tokio::test]
    async fn selector_all_fail() {
        let root = BehaviorTreeNode::Selector {
            id: NodeId::root("sel"),
            name: "sel".into(),
            children: vec![action_node(AlwaysFailure), action_node(AlwaysFailure)],
        };
        let mut rt = BehaviorTreeRuntime::new(root, Blackboard::new(), Arc::new(()));
        assert_eq!(rt.tick().await, NodeResult::Failure);
    }

    #[tokio::test]
    async fn selector_waits_for_running_child_then_succeeds() {
        let root = BehaviorTreeNode::Selector {
            id: NodeId::root("sel"),
            name: "sel".into(),
            children: vec![action_node(DelayedSuccess), action_node(AlwaysSuccess)],
        };
        let mut rt = BehaviorTreeRuntime::new(root, Blackboard::new(), Arc::new(()));

        assert_eq!(rt.tick().await, NodeResult::Running);
        assert_eq!(rt.handles.len(), 1);

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        assert_eq!(rt.tick().await, NodeResult::Success);
        assert!(rt.handles.is_empty());
    }

    #[tokio::test]
    async fn selector_long_running_failure_falls_through() {
        let root = BehaviorTreeNode::Selector {
            id: NodeId::root("sel"),
            name: "sel".into(),
            children: vec![action_node(DelayedFailure), action_node(AlwaysSuccess)],
        };
        let mut rt = BehaviorTreeRuntime::new(root, Blackboard::new(), Arc::new(()));

        assert_eq!(rt.tick().await, NodeResult::Running);
        assert_eq!(rt.handles.len(), 1);

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        assert_eq!(rt.tick().await, NodeResult::Success);
        assert!(rt.handles.is_empty());
    }

    #[tokio::test]
    async fn selector_remembers_running_child_across_ticks() {
        let flag = Arc::new(AtomicBool::new(false));
        let root = BehaviorTreeNode::Selector {
            id: NodeId::root("sel"),
            name: "sel".into(),
            children: vec![
                BehaviorTreeNode::Condition {
                    id: NodeId::root("gate"),
                    name: "gate".into(),
                    condition: Arc::new(FlagCondition(flag.clone())),
                    true_branch: Box::new(action_node(AlwaysSuccess)),
                    false_branch: None,
                },
                action_node(DelayedSuccess),
            ],
        };
        let mut rt = BehaviorTreeRuntime::new(root, Blackboard::new(), Arc::new(()));

        assert_eq!(rt.tick().await, NodeResult::Running);
        flag.store(true, Ordering::SeqCst);

        assert_eq!(rt.tick().await, NodeResult::Running);
        assert_eq!(rt.handles.len(), 1);

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        assert_eq!(rt.tick().await, NodeResult::Success);
        assert!(rt.handles.is_empty());
        assert!(rt.active_children.is_empty());
    }

    #[tokio::test]
    async fn condition_locks_running_branch_until_terminal() {
        let flag = Arc::new(AtomicBool::new(true));
        let root = BehaviorTreeNode::Condition {
            id: NodeId::root("gate"),
            name: "gate".into(),
            condition: Arc::new(FlagCondition(flag.clone())),
            true_branch: Box::new(action_node(DelayedSuccess)),
            false_branch: None,
        };
        let mut rt = BehaviorTreeRuntime::new(root, Blackboard::new(), Arc::new(()));

        assert_eq!(rt.tick().await, NodeResult::Running);
        flag.store(false, Ordering::SeqCst);

        assert_eq!(rt.tick().await, NodeResult::Running);

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        assert_eq!(rt.tick().await, NodeResult::Success);
        assert!(rt.handles.is_empty());
        assert!(rt.active_children.is_empty());
    }

    #[tokio::test]
    async fn parallel_all_succeed_waits_for_slow_child() {
        let root = BehaviorTreeNode::Parallel {
            id: NodeId::root("par"),
            name: "par".into(),
            policy: crate::types::ParallelPolicy::AllSucceed,
            children: vec![action_node(AlwaysSuccess), action_node(DelayedSuccess)],
        };
        let mut rt = BehaviorTreeRuntime::new(root, Blackboard::new(), Arc::new(()));

        assert_eq!(rt.tick().await, NodeResult::Running);
        assert_eq!(rt.handles.len(), 1);

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        assert_eq!(rt.tick().await, NodeResult::Success);
        assert!(rt.handles.is_empty());
    }

    #[tokio::test]
    async fn parallel_all_succeed_fails_on_any_failure() {
        let root = BehaviorTreeNode::Parallel {
            id: NodeId::root("par"),
            name: "par".into(),
            policy: crate::types::ParallelPolicy::AllSucceed,
            children: vec![action_node(AlwaysFailure), action_node(DelayedSuccess)],
        };
        let mut rt = BehaviorTreeRuntime::new(root, Blackboard::new(), Arc::new(()));

        assert_eq!(rt.tick().await, NodeResult::Failure);
    }

    #[tokio::test]
    async fn parallel_first_succeed_short_wins() {
        let root = BehaviorTreeNode::Parallel {
            id: NodeId::root("par"),
            name: "par".into(),
            policy: crate::types::ParallelPolicy::FirstSucceed,
            children: vec![action_node(DelayedSuccess), action_node(AlwaysSuccess)],
        };
        let mut rt = BehaviorTreeRuntime::new(root, Blackboard::new(), Arc::new(()));

        assert_eq!(rt.tick().await, NodeResult::Success);
    }

    #[tokio::test]
    async fn parallel_any_succeed_waits_for_all() {
        let root = BehaviorTreeNode::Parallel {
            id: NodeId::root("par"),
            name: "par".into(),
            policy: crate::types::ParallelPolicy::AnySucceed,
            children: vec![action_node(AlwaysSuccess), action_node(DelayedFailure)],
        };
        let mut rt = BehaviorTreeRuntime::new(root, Blackboard::new(), Arc::new(()));

        assert_eq!(rt.tick().await, NodeResult::Running);

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        assert_eq!(rt.tick().await, NodeResult::Success);
        assert!(rt.handles.is_empty());
    }

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
                    children: vec![action_node(AlwaysFailure), action_node(DelayedSuccess)],
                },
                action_node(AlwaysSuccess),
            ],
        };
        let mut rt = BehaviorTreeRuntime::new(root, Blackboard::new(), Arc::new(()));

        let result = rt.tick().await;
        assert_eq!(result, NodeResult::Success);
        assert!(
            rt.handles.is_empty(),
            "leaked handles: {}",
            rt.handles.len()
        );
    }

    #[tokio::test]
    async fn parallel_all_succeed_cancels_sibling_on_failure() {
        let root = BehaviorTreeNode::Parallel {
            id: NodeId::root("par"),
            name: "par".into(),
            policy: crate::types::ParallelPolicy::AllSucceed,
            children: vec![action_node(AlwaysFailure), action_node(DelayedSuccess)],
        };
        let mut rt = BehaviorTreeRuntime::new(root, Blackboard::new(), Arc::new(()));

        assert_eq!(rt.tick().await, NodeResult::Failure);
        assert!(
            rt.handles.is_empty(),
            "leaked handles: {}",
            rt.handles.len()
        );
    }

    #[tokio::test]
    async fn parallel_first_succeed_cancels_sibling_on_success() {
        let root = BehaviorTreeNode::Parallel {
            id: NodeId::root("par"),
            name: "par".into(),
            policy: crate::types::ParallelPolicy::FirstSucceed,
            children: vec![action_node(DelayedSuccess), action_node(AlwaysSuccess)],
        };
        let mut rt = BehaviorTreeRuntime::new(root, Blackboard::new(), Arc::new(()));

        assert_eq!(rt.tick().await, NodeResult::Success);
        assert!(
            rt.handles.is_empty(),
            "leaked handles: {}",
            rt.handles.len()
        );
    }

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
        let mut rt = BehaviorTreeRuntime::new(root, Blackboard::new(), Arc::new(()));

        assert_eq!(rt.tick().await, NodeResult::Running);
        assert_eq!(rt.handles.len(), 3);

        rt.cancel();
        assert!(
            rt.handles.is_empty(),
            "leaked handles after cancel: {}",
            rt.handles.len()
        );
    }

    #[tokio::test]
    async fn interrupt_resets_for_next_tick() {
        let root = BehaviorTreeNode::Parallel {
            id: NodeId::root("par"),
            name: "par".into(),
            policy: crate::types::ParallelPolicy::AllSucceed,
            children: vec![action_node(DelayedSuccess), action_node(DelayedSuccess)],
        };
        let mut rt = BehaviorTreeRuntime::new(root, Blackboard::new(), Arc::new(()));

        // First tick — two async actions spawned.
        assert_eq!(rt.tick().await, NodeResult::Running);
        assert_eq!(rt.handles.len(), 2);

        // Interrupt — aborts all handles, resets cancellation token.
        rt.interrupt();
        assert!(rt.handles.is_empty());

        // Next tick must run cleanly from root (not return Failure due to cancelled token).
        // The actions are new invocations so they go through the spawn path again.
        let r = rt.tick().await;
        assert!(
            r == NodeResult::Running,
            "expected Running after interrupt, got {:?}",
            r
        );
        assert_eq!(rt.handles.len(), 2);
    }

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
                            children: vec![action_node(AlwaysFailure), action_node(DelayedSuccess)],
                        },
                        action_node(DelayedSuccess),
                    ],
                },
                action_node(AlwaysSuccess),
            ],
        };
        let mut rt = BehaviorTreeRuntime::new(root, Blackboard::new(), Arc::new(()));

        assert_eq!(rt.tick().await, NodeResult::Success);
        assert!(
            rt.handles.is_empty(),
            "leaked handles: {}",
            rt.handles.len()
        );
    }

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
                    children: vec![action_node(DelayedFailure), action_node(DelayedSuccess)],
                },
                action_node(AlwaysSuccess),
            ],
        };
        let mut rt = BehaviorTreeRuntime::new(root, Blackboard::new(), Arc::new(()));

        assert_eq!(rt.tick().await, NodeResult::Running);
        assert_eq!(rt.handles.len(), 2);

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        assert_eq!(rt.tick().await, NodeResult::Failure);
        assert!(
            rt.handles.is_empty(),
            "leaked handles: {}",
            rt.handles.len()
        );
    }

    #[tokio::test]
    async fn no_duplicate_handles_across_ticks() {
        let root = BehaviorTreeNode::Parallel {
            id: NodeId::root("par"),
            name: "par".into(),
            policy: crate::types::ParallelPolicy::AllSucceed,
            children: vec![action_node(DelayedSuccess), action_node(DelayedSuccess)],
        };
        let mut rt = BehaviorTreeRuntime::new(root, Blackboard::new(), Arc::new(()));

        for _ in 0..5 {
            assert_eq!(rt.tick().await, NodeResult::Running);
            assert_eq!(rt.handles.len(), 2, "handles grew unexpectedly");
        }

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert_eq!(rt.tick().await, NodeResult::Success);
        assert!(rt.handles.is_empty());
    }

    #[cfg(feature = "watch")]
    #[derive(Debug)]
    struct WatchAction {
        key: &'static str,
    }

    #[cfg(feature = "watch")]
    #[async_trait]
    impl AsyncBehaviorNode<()> for WatchAction {
        fn name(&self) -> &str {
            "WatchAction"
        }
        async fn execute(&self, ctx: AsyncExecutionContext<()>) -> ActionResult {
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
            Arc::new(()),
        );

        assert_eq!(rt.tick().await, NodeResult::Running);
        assert_eq!(rt.handles.len(), 1);

        bb.insert_key::<SIGNAL>(1).await;

        tokio::time::sleep(std::time::Duration::from_millis(10)).await;

        assert_eq!(rt.tick().await, NodeResult::Success);
        assert!(rt.handles.is_empty());
    }

    #[tokio::test]
    async fn selector_short_success_skips_long_child() {
        let root = BehaviorTreeNode::Selector {
            id: NodeId::root("sel"),
            name: "sel".into(),
            children: vec![action_node(AlwaysSuccess), action_node(DelayedSuccess)],
        };
        let mut rt = BehaviorTreeRuntime::new(root, Blackboard::new(), Arc::new(()));

        assert_eq!(rt.tick().await, NodeResult::Success);
        assert!(rt.handles.is_empty());
    }
}
