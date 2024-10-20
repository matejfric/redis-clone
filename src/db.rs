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
/// db.insert("apples".to_string(), Bytes::from("10"));
/// let apples = db.get("apples").unwrap();
/// println!("Apples: {}", std::str::from_utf8(&apples).unwrap());
/// ```
///
/// If this didnt work, try the following:
///```
/// // type DB = Arc<Mutex<HashMap<String, Bytes>>>;
/// // let mut db = db.lock().unwrap();
/// // db.insert(cmd.key().to_string(), cmd.value().clone());
///```
///
/// OPTIONAL TODO: Sharded database https://tokio.rs/tokio/tutorial/shared-state#mutex-sharding
#[derive(Clone)]
pub struct DB {
    data: Arc<Mutex<HashMap<String, Bytes>>>,
}

impl DB {
    /// Initialize a new database.
    /// TODO: This should be a singleton.
    pub fn new() -> Self {
        DB {
            data: Arc::new(Mutex::new(HashMap::new())),
        }
    }
    /// Thread-safe insert.
    pub fn insert(&self, key: String, value: Bytes) {
        // Before using the database, lock it.
        let mut db = self.data.lock().unwrap();
        db.insert(key, value);
    }
    /// Thread-safe get.
    pub fn get(&self, key: &str) -> Option<Bytes> {
        let db = self.data.lock().unwrap();
        db.get(key).cloned()
    }
    pub fn remove(&self, key: &str) -> Option<Bytes> {
        let mut db = self.data.lock().unwrap();
        db.remove(key)
    }
    pub fn len(&self) -> usize {
        let db = self.data.lock().unwrap();
        db.len()
    }
    pub fn is_empty(&self) -> bool {
        let db = self.data.lock().unwrap();
        db.is_empty()
    }
    pub fn flush(&self) {
        let mut db = self.data.lock().unwrap();
        db.clear();
    }
    pub fn keys(&self) -> Vec<String> {
        let db = self.data.lock().unwrap();
        db.keys().cloned().collect()
    }
    pub fn values(&self) -> Vec<Bytes> {
        let db = self.data.lock().unwrap();
        db.values().cloned().collect()
    }
    pub fn increment(&self, key: &str) -> Option<Bytes> {
        // TODO: This is extremely inefficient.
        let mut db = self.data.lock().unwrap();
        let value = db.entry(key.to_string()).or_insert(Bytes::from("0"));
        let new_value = match std::str::from_utf8(value) {
            Ok(s) => s.parse::<i32>().unwrap() + 1,
            Err(_) => 1, // TODO: handle this?
        };
        *value = Bytes::from(new_value.to_string());
        self.insert(key.to_string(), value.clone());
        Some(value.clone())
    }
}
