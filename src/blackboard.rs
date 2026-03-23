//! Simple async blackboard implementation using SCC's HashMap with typed access.

use scc::HashMap;
use serde::{de::DeserializeOwned, Serialize};
use tokio::sync::RwLock;
use std::any::Any;
use std::sync::Arc;
//re-export serde_json for convenience
pub use serde_json;

/// Trait for values that can be stored in blackboard.
///
/// All stored values must be serializable and deserializable so that
/// bridges (Lua, HTTP) can read and write them generically.
pub trait BlackboardValue: Any + Send + Sync {
    fn to_json(&self) -> Option<serde_json::Value>;

    /// Get type name for debugging
    fn type_name(&self) -> &'static str;

    /// Convert this value into a Lua value.
    ///
    /// The blanket impl uses `lua.to_value(self)` directly (no JSON allocation).
    #[cfg(feature = "lua")]
    fn to_lua(&self, lua: &mlua::Lua) -> mlua::Result<mlua::Value>;

    /// Update this value in-place from a Lua value.
    #[cfg(feature = "lua")]
    fn update_from_lua(&mut self, _lua: &mlua::Lua, _val: mlua::Value) -> mlua::Result<()>;

    /// Return `self` as `&dyn Any` for typed downcasting.
    fn as_any(&self) -> &dyn Any;

    /// Clone this value into a new heap allocation.
    fn clone_box(&self) -> Box<dyn BlackboardValue>;
}

/// Trait for typed blackboard keys that associate a string key with a specific type
pub trait BlackboardKey {
    type Value: BlackboardValue + 'static;
    const KEY: &'static str;
}

// Blanket implementation for all Serialize + DeserializeOwned + Clone types.
impl<T> BlackboardValue for T
where
    T: Any + Send + Sync + Serialize + DeserializeOwned + Clone + 'static,
{
    fn to_json(&self) -> Option<serde_json::Value> {
        serde_json::to_value(self).ok()
    }

    fn type_name(&self) -> &'static str {
        std::any::type_name::<T>()
    }

    #[cfg(feature = "lua")]
    fn to_lua(&self, lua: &mlua::Lua) -> mlua::Result<mlua::Value> {
        use mlua::LuaSerdeExt;
        lua.to_value(self)
    }

    #[cfg(feature = "lua")]
    fn update_from_lua(&mut self, lua: &mlua::Lua, val: mlua::Value) -> mlua::Result<()> {
        use mlua::LuaSerdeExt;
        *self = lua.from_value(val)?;
        Ok(())
    }

    fn as_any(&self) -> &dyn Any { self }

    fn clone_box(&self) -> Box<dyn BlackboardValue> { Box::new(self.clone()) }
}

/// Macro to define typed blackboard keys
///
/// Usage:
/// ```
/// use tasktree::define_key;
///
/// define_key!(ASR_SEGMENTS: Vec<String> = "asr/segments");
///
/// // With documentation
/// define_key!(
///     /// Current detection result.
///     DETECTION_RESULT: String = "perception/detection_result"
/// );
/// ```
#[macro_export]
macro_rules! define_key {
    ($(#[$meta:meta])* $name:ident: $type:ty = $key:literal) => {
        $(#[$meta])*
        #[allow(non_camel_case_types)]
        pub struct $name;

        impl $crate::blackboard::BlackboardKey for $name {
            type Value = $type;
            const KEY: &'static str = $key;
        }
    };
}

/// Entry that stores both the value and its type name for debugging
struct BlackboardEntry {
    value: Box<dyn BlackboardValue>,
}

impl std::fmt::Debug for BlackboardEntry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BlackboardEntry")
            .field("type_name", &self.value.type_name())
            .finish()
    }
}

/// Shared blackboard data
struct SharedBlackboardData {
    data: HashMap<String, BlackboardEntry>,
    #[cfg(feature = "watch")]
    watchers: std::collections::HashMap<String, tokio::sync::watch::Sender<Option<Arc<dyn BlackboardValue>>>>,
}

/// Simple concurrent blackboard for sharing typed data between behavior tree nodes.
/// Uses SCC HashMap with async operations and closure-based access.
#[derive(Clone)]
pub struct Blackboard {
    shared: Arc<RwLock<SharedBlackboardData>>,
}

impl std::fmt::Debug for Blackboard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Blackboard")
            .field("shared", &"Arc<RwLock<SharedBlackboardData>>")
            .finish()
    }
}

impl Default for Blackboard {
    fn default() -> Self {
        Self::new()
    }
}

impl Blackboard {
    /// Create a new empty blackboard
    pub fn new() -> Self {
        Self {
            shared: Arc::new(RwLock::new(SharedBlackboardData {
                data: HashMap::new(),
                #[cfg(feature = "watch")]
                watchers: std::collections::HashMap::new(),
            })),
        }
    }

    /// Notify all watch channel subscribers for a key.
    #[cfg(feature = "watch")]
    async fn notify(&self, key: &str, val: Option<Arc<dyn BlackboardValue>>) {
        let shared = self.shared.read().await;
        if let Some(tx) = shared.watchers.get(key) {
            let _ = tx.send(val);
        }
    }

    #[cfg(not(feature = "watch"))]
    async fn notify(&self, _key: &str, _val: Option<Arc<dyn BlackboardValue>>) {}

    /// Read a typed value with a closure.
    /// Panics if the key exists but the type doesn't match.
    async fn read<T: 'static, R, F>(&self, key: &str, reader: F) -> Option<R>
    where
        T: BlackboardValue,
        F: FnOnce(&T) -> R,
    {
        let shared = self.shared.read().await;
        shared.data.read_async(key, |_, entry| {
            let any_ref: &dyn Any = &*entry.value;
            match any_ref.downcast_ref::<T>() {
                Some(typed_value) => Some(reader(typed_value)),
                None => panic!(
                    "Blackboard type mismatch for key '{}': expected type '{}', but found stored type '{}'",
                    key,
                    std::any::type_name::<T>(),
                    entry.value.type_name()
                ),
            }
        }).await.flatten()
    }

    /// Read a typed value using a typed key (cleaner API)
    pub async fn read_key<K: BlackboardKey, R, F>(&self, reader: F) -> Option<R>
    where
        F: FnOnce(&K::Value) -> R,
    {
        self.read(K::KEY, reader).await
    }

    /// Insert a typed value asynchronously (always succeeds, overwrites if exists)
    async fn insert<T: 'static>(&self, key: &str, value: T)
    where
        T: BlackboardValue,
    {
        let arc_val: Arc<dyn BlackboardValue> = Arc::from(value.clone_box());
        let entry = BlackboardEntry { value: Box::new(value) };
        {
            let shared = self.shared.read().await;
            let _ = shared.data.upsert_async(key.to_string(), entry).await;
        }
        self.notify(key, Some(arc_val)).await;
    }

    /// Insert a typed value using a typed key (cleaner API)
    pub async fn insert_key<K: BlackboardKey>(&self, value: K::Value) {
        self.insert(K::KEY, value).await;
    }

    /// Update a typed value asynchronously with a closure (mutable access).
    /// Returns None if key doesn't exist, panics if type doesn't match.
    async fn update<T: 'static, F, R>(&self, key: &str, updater: F) -> Option<R>
    where
        T: BlackboardValue,
        F: FnOnce(&mut T) -> R,
    {
        let shared = self.shared.read().await;
        let result = shared.data.update_async(key, |_, entry| {
            let any_mut: &mut dyn Any = &mut *entry.value;
            match any_mut.downcast_mut::<T>() {
                Some(value) => Some(updater(value)),
                None => panic!(
                    "Blackboard type mismatch for key '{}': expected type '{}', but found stored type '{}'",
                    key,
                    std::any::type_name::<T>(),
                    entry.value.type_name()
                ),
            }
        }).await.flatten();

        if result.is_some() {
            let arc_val = shared.data
                .read_async(key, |_, entry| Arc::from(entry.value.clone_box()))
                .await;
            drop(shared);
            self.notify(key, arc_val).await;
        }

        result
    }

    /// Update a typed value using a typed key (cleaner API)
    pub async fn update_key<K: BlackboardKey, F, R>(&self, updater: F) -> Option<R>
    where
        F: FnOnce(&mut K::Value) -> R,
    {
        self.update(K::KEY, updater).await
    }

    /// Ensure key exists (create with Default if needed) and apply updater function.
    pub async fn ensure_and_update_key<K: BlackboardKey, F, R>(&self, updater: F) -> R
    where
        K::Value: Default,
        F: FnOnce(&mut K::Value) -> R,
    {
        let shared = self.shared.read().await;
        let entry = shared.data.entry_async(K::KEY.to_string()).await;

        let result = match entry {
            scc::hash_map::Entry::Occupied(mut occupied) => {
                let any_mut: &mut dyn Any = &mut *occupied.get_mut().value;
                match any_mut.downcast_mut::<K::Value>() {
                    Some(value) => updater(value),
                    None => panic!(
                        "Blackboard type mismatch for key '{}': expected type '{}', but found stored type '{}'",
                        K::KEY,
                        std::any::type_name::<K::Value>(),
                        occupied.get().value.type_name()
                    ),
                }
            }
            scc::hash_map::Entry::Vacant(vacant) => {
                let mut default_value = K::Value::default();
                let result = updater(&mut default_value);
                vacant.insert_entry(BlackboardEntry { value: Box::new(default_value) });
                result
            }
        };

        let arc_val = shared.data
            .read_async(K::KEY, |_, entry| Arc::from(entry.value.clone_box()))
            .await;
        drop(shared);
        self.notify(K::KEY, arc_val).await;

        result
    }

    /// Ensure key exists (create with constructor if needed) and read with reader function.
    pub async fn ensure_and_read_key<K: BlackboardKey, C, R, T>(&self, constructor: C, reader: R) -> T
    where
        C: FnOnce() -> K::Value,
        R: FnOnce(&K::Value) -> T,
    {
        let shared = self.shared.read().await;
        let entry = shared.data.entry_async(K::KEY.to_string()).await;

        match entry {
            scc::hash_map::Entry::Occupied(occupied) => {
                let any_ref: &dyn Any = &*occupied.get().value;
                match any_ref.downcast_ref::<K::Value>() {
                    Some(value) => reader(value),
                    None => panic!(
                        "Blackboard type mismatch for key '{}': expected type '{}', but found stored type '{}'",
                        K::KEY,
                        std::any::type_name::<K::Value>(),
                        occupied.get().value.type_name()
                    ),
                }
            }
            scc::hash_map::Entry::Vacant(vacant) => {
                let new_value = constructor();
                let result = reader(&new_value);
                vacant.insert_entry(BlackboardEntry { value: Box::new(new_value) });
                result
            }
        }
    }

    /// Remove a key-value pair
    async fn remove_impl(&self, key: &str) -> Option<BlackboardEntry> {
        let shared = self.shared.read().await;
        shared.data.remove_async(key).await.map(|(_, entry)| entry)
    }

    /// Remove a typed value. Panics if the key exists but type doesn't match.
    async fn remove_typed<T: 'static>(&self, key: &str) -> Option<T>
    where
        T: BlackboardValue,
    {
        self.remove_impl(key).await.map(|entry| {
            let type_name = entry.value.type_name();
            let any: Box<dyn Any> = entry.value;
            match any.downcast::<T>() {
                Ok(typed_val) => *typed_val,
                Err(_) => panic!(
                    "Blackboard type mismatch for key '{}': expected type '{}', but found stored type '{}'",
                    key,
                    std::any::type_name::<T>(),
                    type_name
                ),
            }
        })
    }

    /// Check if a key exists
    pub async fn contains(&self, key: &str) -> bool {
        let shared = self.shared.read().await;
        shared.data.contains_async(key).await
    }

    /// Check if a typed key exists (cleaner API)
    pub async fn contains_key<K: BlackboardKey>(&self) -> bool {
        self.contains(K::KEY).await
    }

    /// Remove using a typed key (cleaner API)
    pub async fn remove_key<K: BlackboardKey>(&self) -> Option<K::Value> {
        self.remove_typed(K::KEY).await
    }

    /// Get the number of entries
    pub async fn len(&self) -> usize {
        let shared = self.shared.read().await;
        shared.data.len()
    }

    /// Check if empty
    pub async fn is_empty(&self) -> bool {
        let shared = self.shared.read().await;
        shared.data.is_empty()
    }

    /// Iterate over all blackboard entries with a callback.
    /// The callback receives `(key: &str, type_name: &str)` for each entry.
    pub async fn scan_entries<F>(&self, mut callback: F)
    where
        F: FnMut(&str, &str),
    {
        let shared = self.shared.read().await;
        shared.data.iter_async(|key, entry| {
            callback(key, entry.value.type_name());
            true
        }).await;
    }

    /// Get metadata about all blackboard keys for HTTP API
    pub async fn get_keys_metadata(&self) -> Vec<BlackboardKeyMetadata> {
        let mut keys = Vec::new();
        self.scan_entries(|key, type_name| {
            keys.push(BlackboardKeyMetadata {
                key: key.to_string(),
                type_name: type_name.to_string(),
            });
        }).await;
        keys.sort_by(|a, b| a.key.cmp(&b.key));
        keys
    }

    /// Subscribe to changes on a specific key.
    ///
    /// The channel is seeded with the current value if the key already exists,
    /// `None` otherwise. Call `receiver.changed().await` to wait for the next write.
    #[cfg(feature = "watch")]
    pub async fn watch(&self, key: &str) -> tokio::sync::watch::Receiver<Option<Arc<dyn BlackboardValue>>> {
        let mut shared = self.shared.write().await;
        if let Some(tx) = shared.watchers.get(key) {
            return tx.subscribe();
        }
        // Seed with current value if the key exists.
        let current = shared.data
            .read_async(key, |_, entry| Arc::from(entry.value.clone_box()))
            .await;
        let (tx, rx) = tokio::sync::watch::channel(current);
        shared.watchers.insert(key.to_string(), tx);
        rx
    }

    /// Subscribe to changes on a typed key.
    #[cfg(feature = "watch")]
    pub async fn watch_key<K: BlackboardKey>(
        &self,
    ) -> tokio::sync::watch::Receiver<Option<Arc<dyn BlackboardValue>>> {
        self.watch(K::KEY).await
    }

    /// Read a blackboard value and convert it to a Lua value.
    /// Returns `Nil` if the key does not exist.
    #[cfg(feature = "lua")]
    pub async fn get_as_lua(&self, lua: &mlua::Lua, key: &str) -> mlua::Result<mlua::Value> {
        let shared = self.shared.read().await;
        let result = shared.data
            .read_async(key, |_, entry| entry.value.to_lua(lua))
            .await;
        match result {
            Some(r) => r,
            None => Ok(mlua::Value::Nil),
        }
    }

    /// Write a Lua value into the blackboard.
    ///
    /// If the key already exists the stored value is updated in-place via
    /// `update_from_lua` (preserving the Rust type). If the key is new the
    /// Lua value is converted to `serde_json::Value` and stored.
    #[cfg(feature = "lua")]
    pub async fn set_from_lua(&self, lua: &mlua::Lua, key: &str, val: mlua::Value) -> mlua::Result<()> {
        use mlua::LuaSerdeExt;
        let mut cell = Some(val.clone());
        let shared = self.shared.read().await;

        let updated = shared.data
            .update_async(key, |_, entry| {
                entry.value.update_from_lua(lua, cell.take().expect("called once"))
            })
            .await;

        match updated {
            Some(result) => {
                let arc_val = shared.data
                    .read_async(key, |_, entry| Arc::from(entry.value.clone_box()))
                    .await;
                drop(shared);
                self.notify(key, arc_val).await;
                result
            }
            None => {
                drop(shared);
                let json: serde_json::Value = lua.from_value(val)?;
                self.insert(key, json).await;
                Ok(())
            }
        }
    }

    /// Get JSON value and type name for a specific key (HTTP GET endpoint).
    pub async fn get_json_value_with_type(&self, key: &str) -> Option<(serde_json::Value, String)> {
        let shared = self.shared.read().await;
        shared.data
            .read_async(key, |_, entry| {
                let type_name = entry.value.type_name().to_string();
                entry.value.to_json().map(|value| (value, type_name))
            })
            .await
            .flatten()
    }
}

/// Metadata about a blackboard key
#[derive(Debug, Clone)]
pub struct BlackboardKeyMetadata {
    pub key: String,
    pub type_name: String,
}

/// Response for a single blackboard key with its value
#[derive(Debug, Clone)]
pub struct BlackboardKeyValue {
    pub key: String,
    pub type_name: String,
    pub value: serde_json::Value,
}

#[cfg(test)]
mod tests {
    use super::*;

    // Define test keys
    define_key!(INT_VALUE: i32 = "test/int_value");
    define_key!(STRING_VALUE: String = "test/string_value");
    define_key!(TEST_COUNTER: i32 = "test/counter");
    define_key!(WATCH_VAL: i32 = "test/watch_val");

    #[tokio::test]
    async fn test_blackboard_basic() {
        let blackboard = Blackboard::new();

        blackboard.insert_key::<INT_VALUE>(42).await;
        blackboard.insert_key::<STRING_VALUE>("hello".to_string()).await;

        let int_val = blackboard.read_key::<INT_VALUE, _, _>(|v| *v).await.unwrap();
        assert_eq!(int_val, 42);

        let string_val = blackboard.read_key::<STRING_VALUE, _, _>(|v| v.clone()).await.unwrap();
        assert_eq!(string_val, "hello");

        assert!(blackboard.contains_key::<INT_VALUE>().await);
        assert!(!blackboard.contains_key::<TEST_COUNTER>().await);

        blackboard.remove_key::<INT_VALUE>().await;
        assert!(!blackboard.contains_key::<INT_VALUE>().await);
    }

    #[tokio::test]
    async fn test_blackboard_update() {
        let blackboard = Blackboard::new();

        blackboard.insert_key::<TEST_COUNTER>(0i32).await;

        let result = blackboard
            .update_key::<TEST_COUNTER, _, _>(|v| { *v += 10; *v })
            .await;
        assert_eq!(result.unwrap(), 10);

        let current = blackboard.read_key::<TEST_COUNTER, _, _>(|v| *v).await.unwrap();
        assert_eq!(current, 10);
    }

    // ── watcher tests ─────────────────────────────────────────────────────

    // Helper: downcast the Arc<dyn BlackboardValue> in a watch receiver to i32.
    fn downcast_i32(arc: &Arc<dyn BlackboardValue>) -> i32 {
        *arc.as_any().downcast_ref::<i32>().expect("expected i32")
    }

    #[cfg(feature = "watch")]
    #[tokio::test]
    async fn watch_receives_initial_none_for_missing_key() {
        let bb = Blackboard::new();
        let rx = bb.watch_key::<WATCH_VAL>().await;
        assert!(rx.borrow().is_none());
    }

    #[cfg(feature = "watch")]
    #[tokio::test]
    async fn watch_seeds_with_current_value_if_key_exists() {
        let bb = Blackboard::new();
        bb.insert_key::<WATCH_VAL>(7).await;
        let rx = bb.watch_key::<WATCH_VAL>().await;
        let val = downcast_i32(rx.borrow().as_ref().unwrap());
        assert_eq!(val, 7);
    }

    #[cfg(feature = "watch")]
    #[tokio::test]
    async fn watch_notified_on_insert() {
        let bb = Blackboard::new();
        let mut rx = bb.watch_key::<WATCH_VAL>().await;

        bb.insert_key::<WATCH_VAL>(42).await;

        rx.changed().await.unwrap();
        let val = downcast_i32(rx.borrow().as_ref().unwrap());
        assert_eq!(val, 42);
    }

    #[cfg(feature = "watch")]
    #[tokio::test]
    async fn watch_notified_on_update() {
        let bb = Blackboard::new();
        bb.insert_key::<WATCH_VAL>(1).await;
        let mut rx = bb.watch_key::<WATCH_VAL>().await;

        bb.update_key::<WATCH_VAL, _, _>(|v| *v += 9).await;

        rx.changed().await.unwrap();
        let val = downcast_i32(rx.borrow().as_ref().unwrap());
        assert_eq!(val, 10);
    }

    #[cfg(feature = "watch")]
    #[tokio::test]
    async fn watch_multiple_subscribers_all_notified() {
        let bb = Blackboard::new();
        let mut rx1 = bb.watch_key::<WATCH_VAL>().await;
        let mut rx2 = bb.watch_key::<WATCH_VAL>().await;

        bb.insert_key::<WATCH_VAL>(99).await;

        rx1.changed().await.unwrap();
        rx2.changed().await.unwrap();

        assert_eq!(downcast_i32(rx1.borrow().as_ref().unwrap()), 99);
        assert_eq!(downcast_i32(rx2.borrow().as_ref().unwrap()), 99);
    }

    #[cfg(feature = "watch")]
    #[tokio::test]
    async fn watch_multiple_writes_all_delivered() {
        let bb = Blackboard::new();
        let mut rx = bb.watch_key::<WATCH_VAL>().await;

        for i in 1i32..=3 {
            bb.insert_key::<WATCH_VAL>(i).await;
            rx.changed().await.unwrap();
            assert_eq!(downcast_i32(rx.borrow().as_ref().unwrap()), i);
        }
    }

    #[cfg(feature = "watch")]
    #[tokio::test]
    async fn watch_by_string_key() {
        let bb = Blackboard::new();
        let mut rx = bb.watch("test/watch_val").await;

        bb.insert_key::<WATCH_VAL>(55).await;

        rx.changed().await.unwrap();
        let val = downcast_i32(rx.borrow().as_ref().unwrap());
        assert_eq!(val, 55);
    }
}