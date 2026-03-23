//! Lua scripting integration for tasktree.
//!
//! Provides [`LuaRuntime`] which manages a Lua VM, registers actions and
//! conditions as Lua functions, and builds behavior trees from a Lua table DSL.

use std::{collections::HashMap, fmt, sync::Arc};

use mlua::{Function, Lua, Table, Value};

use crate::{
    blackboard::Blackboard,
    condition::Condition,
    error::RobotBTError,
    node::AsyncBehaviorNode,
    runtime::BehaviorTreeRuntime,
    tree::{BehaviorTreeNode, NodeId},
    types::{ActionResult, AsyncExecutionContext, ParallelPolicy},
};

// ── LuaBlackboard userdata ────────────────────────────────────────────────────

#[derive(Clone)]
struct LuaBlackboard(Blackboard);

impl mlua::UserData for LuaBlackboard {
    fn add_methods<M: mlua::UserDataMethods<Self>>(methods: &mut M) {
        methods.add_async_method("get", |lua, this, key: String| {
            let bb = this.0.clone();
            async move { bb.get_as_lua(&lua, &key).await }
        });

        methods.add_async_method("set", |lua, this, (key, val): (String, Value)| {
            let bb = this.0.clone();
            async move { bb.set_from_lua(&lua, &key, val).await }
        });
    }
}

// ── LuaAction ────────────────────────────────────────────────────────────────

struct LuaAction {
    name: String,
    #[allow(dead_code)]
    lua: Arc<Lua>,
    func: Function,
}

impl fmt::Debug for LuaAction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "LuaAction({})", self.name)
    }
}

#[async_trait::async_trait]
impl AsyncBehaviorNode for LuaAction {
    fn name(&self) -> &str {
        &self.name
    }

    async fn execute(&self, ctx: AsyncExecutionContext) -> ActionResult {
        let bb = LuaBlackboard(ctx.blackboard);
        match self.func.call_async::<bool>(bb).await {
            Ok(true) => ActionResult::Success,
            Ok(false) => ActionResult::Failure,
            Err(e) => {
                tracing::error!("LuaAction '{}' error: {}", self.name, e);
                ActionResult::Failure
            }
        }
    }
}

// ── LuaCondition ─────────────────────────────────────────────────────────────

struct LuaCondition {
    name: String,
    #[allow(dead_code)]
    lua: Arc<Lua>,
    func: Function,
}

impl fmt::Debug for LuaCondition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "LuaCondition({})", self.name)
    }
}

#[async_trait::async_trait]
impl Condition for LuaCondition {
    fn name(&self) -> &str {
        &self.name
    }

    async fn evaluate(&self, blackboard: &Blackboard) -> bool {
        let bb = LuaBlackboard(blackboard.clone());
        match self.func.call_async::<bool>(bb).await {
            Ok(result) => result,
            Err(e) => {
                tracing::error!("LuaCondition '{}' error: {}", self.name, e);
                false
            }
        }
    }
}

// ── LuaRuntime ───────────────────────────────────────────────────────────────

/// Lua-driven behavior tree runtime.
///
/// Register actions and conditions as Lua functions, then load and run a tree
/// described by a Lua table DSL.
///
/// # Lua DSL example
///
/// ```lua
/// tasktree.register_action("move", function(bb)
///     local target = bb:get("target")
///     -- do work …
///     return true  -- true = Success, false = Failure
/// end)
///
/// return {
///     type = "Sequence",
///     name = "root",
///     children = {
///         { type = "Action", name = "move" },
///     },
/// }
/// ```
pub struct LuaRuntime {
    lua: Arc<Lua>,
    actions: HashMap<String, Function>,
    conditions: HashMap<String, Function>,
}

impl LuaRuntime {
    /// Create a new Lua VM.
    pub fn new() -> Result<Self, RobotBTError> {
        Ok(Self {
            lua: Arc::new(Lua::new()),
            actions: HashMap::new(),
            conditions: HashMap::new(),
        })
    }

    /// Execute a Lua script.
    ///
    /// The script may call `tasktree.register_action(name, fn)` and
    /// `tasktree.register_condition(name, fn)` to register handlers.
    pub fn load_script(&mut self, script: &str) -> Result<(), RobotBTError> {
        use std::sync::Mutex;

        let pending_actions: Arc<Mutex<Vec<(String, Function)>>> =
            Arc::new(Mutex::new(Vec::new()));
        let pending_conditions: Arc<Mutex<Vec<(String, Function)>>> =
            Arc::new(Mutex::new(Vec::new()));

        let pa = Arc::clone(&pending_actions);
        let pc = Arc::clone(&pending_conditions);

        let tt = self.lua.create_table()?;

        tt.set(
            "register_action",
            self.lua.create_function(move |_lua, (name, func): (String, Function)| {
                pa.lock().unwrap().push((name, func));
                Ok(())
            })?,
        )?;

        tt.set(
            "register_condition",
            self.lua.create_function(move |_lua, (name, func): (String, Function)| {
                pc.lock().unwrap().push((name, func));
                Ok(())
            })?,
        )?;

        self.lua.globals().set("tasktree", tt)?;
        self.lua.load(script).exec()?;

        for (name, func) in pending_actions.lock().unwrap().drain(..) {
            self.actions.insert(name, func);
        }
        for (name, func) in pending_conditions.lock().unwrap().drain(..) {
            self.conditions.insert(name, func);
        }

        Ok(())
    }

    /// Evaluate a Lua expression that returns a tree table and build a runtime.
    pub fn load_tree(
        &self,
        script: &str,
        blackboard: Blackboard,
    ) -> Result<BehaviorTreeRuntime, RobotBTError> {
        let table: Table = self.lua.load(script).eval()?;
        let tree = self.table_to_node(table)?;
        Ok(BehaviorTreeRuntime::new(tree, blackboard))
    }

    // ── tree building ─────────────────────────────────────────────────────

    fn table_to_node(&self, table: Table) -> Result<BehaviorTreeNode, RobotBTError> {
        let node_type: String = table.get("type")?;

        match node_type.as_str() {
            "Action" => {
                let name: String = table.get("name")?;
                let node = self.resolve_action(&name)?;
                Ok(BehaviorTreeNode::Action { id: NodeId::default(), node })
            }

            "Sequence" => {
                let name: String = table.get("name")?;
                let children = self.get_children(table)?;
                Ok(BehaviorTreeNode::Sequence { id: NodeId::default(), name, children })
            }

            "Selector" => {
                let name: String = table.get("name")?;
                let children = self.get_children(table)?;
                Ok(BehaviorTreeNode::Selector { id: NodeId::default(), name, children })
            }

            "Parallel" => {
                let name: String = table.get("name")?;
                let policy = match table.get::<Option<String>>("policy")?.as_deref() {
                    None | Some("AllSucceed") => ParallelPolicy::AllSucceed,
                    Some("FirstSucceed") => ParallelPolicy::FirstSucceed,
                    Some("AnySucceed") => ParallelPolicy::AnySucceed,
                    Some(other) => {
                        return Err(RobotBTError::LuaError(
                            format!("unknown parallel policy '{other}'"),
                        ))
                    }
                };
                let children = self.get_children(table)?;
                Ok(BehaviorTreeNode::Parallel { id: NodeId::default(), name, policy, children })
            }

            "Condition" => {
                let name: String = table.get("name")?;
                let cond_name: String = table.get("condition_name")?;
                let condition = self.resolve_condition(&cond_name)?;
                let true_table: Table = table.get("true_branch")?;
                let true_branch = Box::new(self.table_to_node(true_table)?);
                let false_branch = table
                    .get::<Option<Table>>("false_branch")?
                    .map(|t| self.table_to_node(t).map(Box::new))
                    .transpose()?;
                Ok(BehaviorTreeNode::Condition {
                    id: NodeId::default(),
                    name,
                    condition,
                    true_branch,
                    false_branch,
                })
            }

            other => Err(RobotBTError::LuaError(format!("unknown node type '{other}'"))),
        }
    }

    fn get_children(&self, table: Table) -> Result<Vec<BehaviorTreeNode>, RobotBTError> {
        let children: Table = table.get("children")?;
        let len = children.raw_len();
        let mut result = Vec::with_capacity(len as usize);
        for i in 1..=len {
            let child: Table = children.raw_get(i)?;
            result.push(self.table_to_node(child)?);
        }
        Ok(result)
    }

    // ── resolution ────────────────────────────────────────────────────────

    fn resolve_action(&self, name: &str) -> Result<Arc<dyn AsyncBehaviorNode>, RobotBTError> {
        if let Some(func) = self.actions.get(name) {
            return Ok(Arc::new(LuaAction {
                name: name.to_string(),
                lua: Arc::clone(&self.lua),
                func: func.clone(),
            }));
        }
        crate::registry::resolve_action(name)
            .ok_or_else(|| RobotBTError::UnknownAction { name: name.to_string() })
    }

    fn resolve_condition(&self, name: &str) -> Result<Arc<dyn Condition>, RobotBTError> {
        if let Some(func) = self.conditions.get(name) {
            return Ok(Arc::new(LuaCondition {
                name: name.to_string(),
                lua: Arc::clone(&self.lua),
                func: func.clone(),
            }));
        }
        crate::registry::resolve_condition(name)
            .ok_or_else(|| RobotBTError::UnknownCondition { name: name.to_string() })
    }
}

// ── tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::blackboard::Blackboard;
    use crate::types::NodeResult;

    // ── blackboard bridge ─────────────────────────────────────────────────

    #[tokio::test]
    async fn lua_blackboard_set_and_get_primitives() {
        let bb = Blackboard::new();
        let lua = Lua::new();

        // set from Lua side (new key → stored as serde_json::Value)
        bb.set_from_lua(&lua, "count", Value::Integer(42)).await.unwrap();
        bb.set_from_lua(&lua, "flag", Value::Boolean(true)).await.unwrap();
        bb.set_from_lua(&lua, "label", Value::String(lua.create_string("hello").unwrap())).await.unwrap();

        let count = bb.get_as_lua(&lua, "count").await.unwrap();
        let flag  = bb.get_as_lua(&lua, "flag").await.unwrap();
        let label = bb.get_as_lua(&lua, "label").await.unwrap();

        assert!(matches!(count, Value::Integer(42)));
        assert!(matches!(flag,  Value::Boolean(true)));
        if let Value::String(s) = label { assert_eq!(s.to_str().unwrap(), "hello"); }
        else { panic!("expected string"); }
    }

    #[tokio::test]
    async fn lua_blackboard_get_missing_returns_nil() {
        let bb = Blackboard::new();
        let lua = Lua::new();
        let val = bb.get_as_lua(&lua, "no_such_key").await.unwrap();
        assert!(matches!(val, Value::Nil));
    }

    #[tokio::test]
    async fn lua_blackboard_set_table() {
        let bb = Blackboard::new();
        let lua = Lua::new();

        // Lua table → stored as serde_json::Value
        let table: Table = lua.load(r#"{x = 1, y = 2}"#).eval().unwrap();
        bb.set_from_lua(&lua, "pos", Value::Table(table)).await.unwrap();

        let val = bb.get_as_lua(&lua, "pos").await.unwrap();
        if let Value::Table(t) = val {
            assert_eq!(t.get::<i64>("x").unwrap(), 1);
            assert_eq!(t.get::<i64>("y").unwrap(), 2);
        } else {
            panic!("expected table");
        }
    }

    #[tokio::test]
    async fn lua_blackboard_mutate_rust_owned_value() {
        crate::define_key!(COUNTER: i64 = "test/counter");

        let bb = Blackboard::new();
        bb.insert_key::<COUNTER>(10).await;

        let lua = Lua::new();
        // Lua mutates an existing Rust-typed i64
        bb.set_from_lua(&lua, "test/counter", Value::Integer(99)).await.unwrap();

        let val = bb.read_key::<COUNTER, _, _>(|v| *v).await.unwrap();
        assert_eq!(val, 99);
    }

    // ── LuaRuntime — action ───────────────────────────────────────────────

    #[tokio::test]
    async fn lua_action_success() {
        let mut rt = LuaRuntime::new().unwrap();
        rt.load_script(r#"
            tasktree.register_action("always_ok", function(bb)
                return true
            end)
        "#).unwrap();

        let bb = Blackboard::new();
        let mut runtime = rt.load_tree(r#"
            return { type = "Action", name = "always_ok" }
        "#, bb).unwrap();

        let result = runtime.tick().await;
        assert_eq!(result, NodeResult::Success);
    }

    #[tokio::test]
    async fn lua_action_failure() {
        let mut rt = LuaRuntime::new().unwrap();
        rt.load_script(r#"
            tasktree.register_action("always_fail", function(bb)
                return false
            end)
        "#).unwrap();

        let bb = Blackboard::new();
        let mut runtime = rt.load_tree(r#"
            return { type = "Action", name = "always_fail" }
        "#, bb).unwrap();

        let result = runtime.tick().await;
        assert_eq!(result, NodeResult::Failure);
    }

    // ── LuaRuntime — blackboard read/write in action ──────────────────────

    #[tokio::test]
    async fn lua_action_reads_and_writes_blackboard() {
        let mut rt = LuaRuntime::new().unwrap();
        rt.load_script(r#"
            tasktree.register_action("increment", function(bb)
                local n = bb:get("n")
                if n == nil then n = 0 end
                bb:set("n", n + 1)
                return true
            end)
        "#).unwrap();

        let bb = Blackboard::new();
        let lua = Lua::new();
        bb.set_from_lua(&lua, "n", Value::Integer(5)).await.unwrap();

        let mut runtime = rt.load_tree(r#"
            return { type = "Action", name = "increment" }
        "#, bb.clone()).unwrap();

        runtime.tick().await;

        let val = bb.get_as_lua(&lua, "n").await.unwrap();
        assert!(matches!(val, Value::Integer(6)));
    }

    // ── LuaRuntime — sequence ─────────────────────────────────────────────

    #[tokio::test]
    async fn lua_sequence_all_succeed() {
        let mut rt = LuaRuntime::new().unwrap();
        rt.load_script(r#"
            tasktree.register_action("a", function(bb) return true end)
            tasktree.register_action("b", function(bb) return true end)
        "#).unwrap();

        let bb = Blackboard::new();
        let mut runtime = rt.load_tree(r#"
            return {
                type = "Sequence", name = "seq",
                children = {
                    { type = "Action", name = "a" },
                    { type = "Action", name = "b" },
                }
            }
        "#, bb).unwrap();

        assert_eq!(runtime.tick().await, NodeResult::Success);
    }

    #[tokio::test]
    async fn lua_sequence_short_circuits_on_failure() {
        let mut rt = LuaRuntime::new().unwrap();
        rt.load_script(r#"
            tasktree.register_action("ok",   function(bb) return true  end)
            tasktree.register_action("fail", function(bb) return false end)
        "#).unwrap();

        let bb = Blackboard::new();
        let mut runtime = rt.load_tree(r#"
            return {
                type = "Sequence", name = "seq",
                children = {
                    { type = "Action", name = "ok" },
                    { type = "Action", name = "fail" },
                    { type = "Action", name = "ok" },
                }
            }
        "#, bb).unwrap();

        assert_eq!(runtime.tick().await, NodeResult::Failure);
    }

    // ── LuaRuntime — condition ────────────────────────────────────────────

    #[tokio::test]
    async fn lua_condition_takes_true_branch() {
        let mut rt = LuaRuntime::new().unwrap();
        rt.load_script(r#"
            tasktree.register_condition("is_ready", function(bb) return true end)
            tasktree.register_action("do_work",  function(bb) return true end)
            tasktree.register_action("fallback", function(bb) return false end)
        "#).unwrap();

        let bb = Blackboard::new();
        let mut runtime = rt.load_tree(r#"
            return {
                type = "Condition", name = "check",
                condition_name = "is_ready",
                true_branch  = { type = "Action", name = "do_work" },
                false_branch = { type = "Action", name = "fallback" },
            }
        "#, bb).unwrap();

        assert_eq!(runtime.tick().await, NodeResult::Success);
    }

    // ── LuaRuntime — unknown action ───────────────────────────────────────

    #[test]
    fn lua_load_tree_unknown_action_errors() {
        let rt = LuaRuntime::new().unwrap();
        let bb = Blackboard::new();
        let err = rt.load_tree(r#"
            return { type = "Action", name = "ghost" }
        "#, bb).unwrap_err();
        assert!(matches!(err, RobotBTError::UnknownAction { .. }));
    }
}