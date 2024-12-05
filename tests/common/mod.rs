#![allow(unused)]

use redis_clone::{RedisClient, RedisServer};
use std::sync::{
    atomic::{AtomicU16, Ordering},
    Arc,
};
use tokio::sync::Barrier;

pub const SERVER_ADDR: &str = "127.0.0.1";

// Static atomic counter for generating unique ports
static SERVER_PORT_COUNTER: AtomicU16 = AtomicU16::new(31_415);

/// Test server utility to create isolated server instances
#[derive(Debug, Clone)]
pub struct TestServer {
    port: u16,
    handle: Arc<tokio::task::JoinHandle<()>>,
    shutdown: Arc<tokio::sync::broadcast::Sender<()>>,
}

impl TestServer {
    pub async fn new() -> Self {
        let server_port = SERVER_PORT_COUNTER.fetch_add(1, Ordering::SeqCst);

        let mut server = RedisServer::new(SERVER_ADDR, server_port)
            .await
            .expect("Failed to create Redis server");

        let shutdown = server.get_shutdown_handle();

        let handle = tokio::spawn(async move {
            server.run().await.expect("Failed to run Redis server");
        });

        TestServer {
            port: server_port,
            handle: Arc::new(handle),
            shutdown: Arc::new(shutdown),
        }
    }

    /// Create a new Redis client connected to a test server
    pub async fn create_client(&self) -> redis_clone::RedisClient {
        RedisClient::new(SERVER_ADDR, self.port)
            .await
            .expect("Failed to create Redis client")
    }

    /// Get the port of the running server
    pub fn port(&self) -> u16 {
        self.port
    }

    pub fn addr(&self) -> String {
        format!("{}:{}", SERVER_ADDR, self.port)
    }
}

/// Initializes logger for a test (call at the start of test functions)
pub fn get_or_init_logger() {
    let _ = env_logger::builder()
        .is_test(true)
        .filter_level(log::LevelFilter::Debug)
        .try_init();
}

/// Utility for creating a barrier for synchronizing concurrent tests
pub fn create_barrier(count: usize) -> Arc<Barrier> {
    Arc::new(Barrier::new(count))
}
