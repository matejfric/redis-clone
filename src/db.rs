use std::collections::{BinaryHeap, HashMap};
use std::str;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail};
use bytes::Bytes;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::sync::Mutex; // async mutex, because of the `expiration_task`

use crate::constants::DB_EXPIRATION_CHECK_INTERVAL;

#[derive(Clone, Debug)]
struct ExpirationEntry {
    key: String,
    expiration_time: Instant,
}

// Implement Ord for BinaryHeap to work with earliest expiration first
impl Eq for ExpirationEntry {}

impl PartialEq for ExpirationEntry {
    fn eq(&self, other: &Self) -> bool {
        self.expiration_time == other.expiration_time
    }
}

impl Ord for ExpirationEntry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Reverse order so earliest expiration is at the top
        other.expiration_time.cmp(&self.expiration_time)
    }
}

impl PartialOrd for ExpirationEntry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Debug, Clone)]
struct DBItem {
    value: Bytes,
    expiration: Option<Instant>,
}

impl DBItem {
    fn new(value: Bytes, expiration: Option<Instant>) -> Self {
        Self { value, expiration }
    }
}

/// Redis cache database shared between tasks and threads.
/// Inspired by: https://tokio.rs/tokio/tutorial/shared-state
///
/// ## Example
/// ```
/// let runtime = tokio::runtime::Runtime::new().unwrap();
/// let result = runtime.block_on(async {
///     let db = redis_clone::DB::new();
///     db.set("apples".to_string(), bytes::Bytes::from("10"), None).await;
///     let apples = db.get("apples").await.unwrap();
///     std::str::from_utf8(&apples).unwrap().to_string()
/// });
/// assert_eq!(&result, "10");
/// ```
#[derive(Clone)]
pub struct DB {
    data: Arc<Mutex<HashMap<String, DBItem>>>,
    expiration_queue: Arc<Mutex<BinaryHeap<ExpirationEntry>>>,
    expiration_sender: Sender<()>,
}

impl DB {
    /// Initialize a new database.
    pub fn new() -> Self {
        let (sender, receiver) = channel(1);
        let db = Self {
            data: Arc::new(Mutex::new(HashMap::new())),
            expiration_queue: Arc::new(Mutex::new(BinaryHeap::new())),
            expiration_sender: sender,
        };
        db.start_expiration_task(receiver);
        db
    }

    /// Start a task to handle key expiration.
    /// Runs in the background and removes expired key until it receives a signal to stop.
    fn start_expiration_task(&self, mut receiver: Receiver<()>) {
        let data = Arc::clone(&self.data);
        let expiration_queue = Arc::clone(&self.expiration_queue);

        // Spawn a Tokio task for key expiration
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(DB_EXPIRATION_CHECK_INTERVAL);

            loop {
                // Wait for the next interval tick
                interval.tick().await;

                // Check if we should stop (shutdown signal)
                if receiver.try_recv().is_ok() {
                    break;
                }

                // Remove expired keys
                let now = Instant::now();
                let mut queue = expiration_queue.lock().await;
                let mut expired_keys = Vec::new();
                while let Some(entry) = queue.peek() {
                    if entry.expiration_time <= now {
                        // Remove the top entry
                        let entry = queue.pop().unwrap();
                        expired_keys.push(entry.key);
                    } else {
                        // Queue is sorted, so we can break if not expired
                        break;
                    }
                }
                drop(queue);

                // Remove expired keys from data store
                let mut data_store = data.lock().await;
                for key in expired_keys {
                    data_store.remove(&key);
                }
                drop(data_store);
            }
        });
    }

    pub async fn set(&self, key: String, value: Bytes, duration: Option<Duration>) {
        let expiration_time = duration.map(|d| Instant::now() + d);

        // Lock and insert into data store
        let mut data_store = self.data.lock().await;
        data_store.insert(
            key.clone(),
            DBItem {
                value,
                expiration: expiration_time,
            },
        );

        // If there's an expiration, add to queue
        if let Some(expire) = expiration_time {
            let mut queue = self.expiration_queue.lock().await;
            queue.push(ExpirationEntry {
                key,
                expiration_time: expire,
            });
        }
    }

    pub async fn get(&self, key: &str) -> Option<Bytes> {
        let data_store = self.data.lock().await;
        data_store.get(key).and_then(|item| {
            // Check if not expired
            if item.expiration.map_or(true, |exp| Instant::now() < exp) {
                Some(item.value.clone())
            } else {
                None
            }
        })
    }

    pub async fn expire(&self, key: &str, duration: Duration) -> bool {
        let mut data_store = self.data.lock().await;

        if let Some(item) = data_store.get_mut(key) {
            let new_expiration = Instant::now() + duration;
            item.expiration = Some(new_expiration);

            // Add to expiration queue
            let mut queue = self.expiration_queue.lock().await;
            queue.push(ExpirationEntry {
                key: key.to_string(),
                expiration_time: new_expiration,
            });

            true
        } else {
            false
        }
    }

    /// Check if a key exists in the database.
    pub async fn exists(&self, key: &str) -> bool {
        self.get(key).await.is_some()
    }

    /// Remove a key from the database.
    pub async fn remove(&self, key: &str) -> Option<Bytes> {
        let mut db_guard = self.data.lock().await;
        let value = db_guard.remove(key);
        drop(db_guard);

        // Remove from expiration queue
        let mut queue_guard = self.expiration_queue.lock().await;
        queue_guard.retain(|entry| entry.key != key);
        drop(queue_guard);

        value.map(|item| item.value)
    }

    /// Get the number of key-value pairs in the database.
    pub async fn size(&self) -> usize {
        let db_guard = self.data.lock().await;
        db_guard.len()
    }

    /// Clear the database.
    pub async fn flush(&self) {
        let mut db_guard = self.data.lock().await;
        db_guard.clear(); // Remove all key-value pairs.
        db_guard.shrink_to_fit(); // Free up unused memory.
    }

    /// Get all the keys in the database.
    pub async fn keys(&self, pattern: &str) -> anyhow::Result<Vec<String>> {
        let glob_pattern = glob::Pattern::new(pattern)?;
        let db_guard = self.data.lock().await;
        Ok(db_guard
            .keys()
            .filter(|key| glob_pattern.matches(key))
            .cloned()
            .collect())
    }

    /// Increment a value of key-value pair in the database.
    pub async fn increment(&self, key: &str) -> anyhow::Result<Bytes> {
        let mut db_guard = self.data.lock().await;
        let item = db_guard
            .entry(key.to_string())
            .or_insert(DBItem::new(Bytes::from("0"), None));

        // Check expiration
        if let Some(expiration) = item.expiration {
            if Instant::now() >= expiration {
                db_guard.remove(key);
                bail!("Key has expired");
            }
        }

        let new_value = match str::from_utf8(item.value.as_ref()) {
            Ok(s) => s
                .parse::<i64>()
                .map_err(|e| anyhow!(e))?
                .checked_add(1)
                .ok_or_else(|| anyhow!("Integer overflow"))?,
            Err(e) => bail!(e),
        };

        // Modify the value in place
        item.value = Bytes::from(new_value.to_string());

        drop(db_guard);

        Ok(Bytes::from(new_value.to_string()))
    }

    pub async fn ttl(&self, key: &str) -> Result<Option<Duration>, ()> {
        let db_guard = self.data.lock().await;
        let maybe_item = db_guard.get(key);
        match maybe_item {
            Some(item) => match item.expiration {
                Some(expiration) => {
                    if Instant::now() < expiration {
                        Ok(Some(expiration - Instant::now()))
                    } else {
                        // Key has (just) expired
                        Ok(None)
                    }
                }
                // Key has no expiration
                None => Ok(None),
            },
            // Key does not exist
            None => Err(()),
        }
    }

    /// Shutdown method to stop the expiration task
    pub async fn shutdown(&self) {
        // Send signal to stop the expiration task
        match self.expiration_sender.send(()).await {
            Ok(_) => log::info!("Database shutdown signal sent."),
            Err(e) => log::error!("Error sending database shutdown signal: {:?}", e),
        }
    }
}

impl Default for DB {
    fn default() -> Self {
        Self::new()
    }
}
