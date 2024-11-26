use anyhow::{anyhow, bail};
use bytes::Bytes;
use std::collections::HashMap;
use std::str;
use std::sync::{Arc, Mutex, MutexGuard};

/// Redis cache database shared between tasks and threads.
/// Inspired by: https://tokio.rs/tokio/tutorial/shared-state
///
/// ## Example
/// ```
/// use bytes::Bytes;
/// use redis_clone::db::DB;
///
/// let db = DB::new();
/// db.set("apples".to_string(), Bytes::from("10"));
/// let apples = db.get("apples").unwrap();
/// println!("Apples: {}", std::str::from_utf8(&apples).unwrap());
/// ```
#[derive(Clone, Default)]
pub struct DB {
    data: Arc<Mutex<HashMap<String, Bytes>>>,
}

impl DB {
    /// Initialize a new database.
    pub fn new() -> Self {
        DB {
            data: Arc::new(Mutex::new(HashMap::new())),
        }
    }
    fn get_lock(&self) -> MutexGuard<HashMap<String, Bytes>> {
        self.data.lock().unwrap()
    }
    /// Set a key-value pair in the database.
    pub fn set(&self, key: String, value: Bytes) {
        let mut db = self.get_lock();
        db.insert(key, value);
    }
    /// Get a value from the database.
    pub fn get(&self, key: &str) -> Option<Bytes> {
        let db = self.get_lock();
        db.get(key).cloned()
    }
    /// Check if a key exists in the database.
    pub fn exists(&self, key: &str) -> bool {
        let db = self.get_lock();
        db.contains_key(key)
    }
    /// Remove a key from the database.
    pub fn remove(&self, key: &str) -> Option<Bytes> {
        let mut db = self.get_lock();
        db.remove(key)
    }
    /// Get the number of key-value pairs in the database.
    pub fn len(&self) -> usize {
        let db = self.get_lock();
        db.len()
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
    /// Clear the database.
    pub fn flush(&self) {
        let mut db = self.get_lock();
        db.clear(); // Remove all key-value pairs.
        db.shrink_to_fit(); // Free up unused memory.
    }
    /// Get all the keys in the database.
    pub fn keys(&self) -> Vec<String> {
        let db = self.get_lock();
        db.keys().cloned().collect()
    }
    /// Increment a value of key-value pair in the database.
    pub fn increment(&self, key: &str) -> anyhow::Result<Bytes> {
        let mut db = self.get_lock();
        let value = db.entry(key.to_string()).or_insert(Bytes::from("0"));
        let new_value = match str::from_utf8(value) {
            Ok(s) => s
                .parse::<i64>()
                .map_err(|e| anyhow!(e))?
                .checked_add(1)
                .ok_or_else(|| anyhow!("Integer overflow"))?,
            Err(e) => bail!(e),
        };
        let value = Bytes::from(new_value.to_string());
        // We cannot use `self.set` here, because we would try to acquire the database lock twice.
        db.insert(key.to_string(), value.clone());
        Ok(value)
    }
}
