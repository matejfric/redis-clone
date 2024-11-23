use anyhow::bail;
use bytes::Bytes;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

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
    /// Set a key-value pair in the database.
    pub fn set(&self, key: String, value: Bytes) {
        let mut db = self.data.lock().unwrap();
        db.insert(key, value);
    }
    /// Get a value from the database.
    pub fn get(&self, key: &str) -> Option<Bytes> {
        let db = self.data.lock().unwrap();
        db.get(key).cloned()
    }
    /// Remove a key from the database.
    pub fn remove(&self, key: &str) -> Option<Bytes> {
        let mut db = self.data.lock().unwrap();
        db.remove(key)
    }
    /// Get the number of key-value pairs in the database.
    pub fn len(&self) -> usize {
        let db = self.data.lock().unwrap();
        db.len()
    }
    /// Check if the database is empty.
    pub fn is_empty(&self) -> bool {
        let db = self.data.lock().unwrap();
        db.is_empty()
    }
    /// Clear the database.
    pub fn flush(&self) {
        let mut db = self.data.lock().unwrap();
        db.clear();
    }
    /// Get all the keys in the database.
    pub fn keys(&self) -> Vec<String> {
        let db = self.data.lock().unwrap();
        db.keys().cloned().collect()
    }
    /// Get all the values in the database.
    pub fn values(&self) -> Vec<Bytes> {
        let db = self.data.lock().unwrap();
        db.values().cloned().collect()
    }
    /// Increment a value of key-value pair in the database.
    pub fn increment(&self, key: &str) -> anyhow::Result<Bytes> {
        // TODO: This is extremely inefficient.
        let mut db = self.data.lock().unwrap();
        let value = db.entry(key.to_string()).or_insert(Bytes::from("0"));
        let new_value = match std::str::from_utf8(value) {
            Ok(s) => s.parse::<i64>().unwrap() + 1,
            Err(_) => bail!("Cannot increment non-integer value."),
        };
        *value = Bytes::from(new_value.to_string());
        self.set(key.to_string(), value.clone());
        Ok(value.clone())
    }
}
