use redis_clone::err::RedisCommandError;
use std::sync::atomic::{AtomicU16, Ordering};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::timeout;

use redis_clone::RedisServer;

const SERVER_ADDR: &str = "127.0.0.1";
const CONNECTION_TIMEOUT: Duration = Duration::from_secs(3);

// Each test will have a separate server instance.
static SERVER_PORT_COUNTER: AtomicU16 = AtomicU16::new(31_415);

struct TestClient {
    stream: TcpStream,
}

impl TestClient {
    async fn new() -> Self {
        let server_port = SERVER_PORT_COUNTER.fetch_add(1, Ordering::SeqCst);

        let server = RedisServer::new(SERVER_ADDR, server_port)
            .await
            .expect("Failed to create Redis server.");

        tokio::spawn(async move {
            server.run().await.expect("Failed to run Redis server");
        });

        let stream = timeout(
            CONNECTION_TIMEOUT,
            TcpStream::connect((SERVER_ADDR, server_port)),
        )
        .await
        .expect("Connection timeout")
        .unwrap_or_else(|_| {
            panic!(
                "Failed to connect to Redis server {}:{}",
                SERVER_ADDR, server_port
            )
        });

        TestClient { stream }
    }

    async fn send(&mut self, command: &str) {
        self.stream
            .write_all(command.as_bytes())
            .await
            .expect("Failed to write to server");
        self.stream.flush().await.expect("Failed to flush");
    }

    async fn assert_response(&mut self, expected: &[u8]) {
        let mut response = vec![0; expected.len()];
        self.stream
            .read_exact(&mut response)
            .await
            .expect("Failed to read from server");
        assert_eq!(
            expected,
            &response[..],
            "Unexpected response {}",
            String::from_utf8_lossy(response.as_slice())
        );
    }

    async fn set(&mut self, key: &str, value: &str) {
        self.send(
            format!(
                "*3\r\n$3\r\nSET\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
                key.len(),
                key,
                value.len(),
                value
            )
            .as_str(),
        )
        .await;
        self.assert_response(b"+OK\r\n").await;
    }

    async fn send_get(&mut self, key: &str) {
        self.send(format!("*2\r\n$3\r\nGET\r\n${}\r\n{}\r\n", key.len(), key).as_str())
            .await;
    }

    async fn send_exists(&mut self, key: Vec<&str>) {
        let mut command = String::from("*");
        command.push_str(&(key.len() + 1).to_string());
        command.push_str("\r\n");
        command.push_str("$6\r\nEXISTS\r\n");
        for k in key {
            command.push('$');
            command.push_str(&k.len().to_string());
            command.push_str("\r\n");
            command.push_str(k);
            command.push_str("\r\n");
        }
        self.send(command.as_str()).await;
    }

    async fn send_del(&mut self, key: Vec<&str>) {
        let mut command = String::from("*");
        command.push_str(&(key.len() + 1).to_string());
        command.push_str("\r\n$3\r\nDEL\r\n");
        for k in key {
            command.push('$');
            command.push_str(&k.len().to_string());
            command.push_str("\r\n");
            command.push_str(k);
            command.push_str("\r\n");
        }
        self.send(command.as_str()).await;
    }

    async fn send_incr(&mut self, key: &str) {
        self.send(format!("*2\r\n$4\r\nINCR\r\n${}\r\n{}\r\n", key.len(), key).as_str())
            .await;
    }

    async fn assert_size(&mut self, expected: i64) {
        self.send("*1\r\n$6\r\nDBSIZE\r\n").await;
        self.assert_response(format!(":{}\r\n", expected).as_bytes())
            .await;
    }
}

#[tokio::test]
async fn get_before_set() {
    let mut client = TestClient::new().await;
    client.send_get("key42").await;
    client.assert_response(b"$-1\r\n").await;
}

#[tokio::test]
async fn set_and_get_value() {
    let mut client = TestClient::new().await;
    client.set("hello", "world").await;
    client.send_get("hello").await;
    client.assert_response(b"$5\r\nworld\r\n").await;
}

#[tokio::test]
async fn overwrite_key() {
    let mut client = TestClient::new().await;

    // Set a key
    client.set("key1", "val1").await;

    // Overwrite the key with a new value
    client.set("key1", "val2").await;

    // Get the key
    client.send_get("key1").await;
    client.assert_response(b"$4\r\nval2\r\n").await;
}

#[tokio::test]
async fn ping_command() {
    let mut client = TestClient::new().await;

    // Send PING
    client.send("*1\r\n$4\r\nPING\r\n").await;

    // Read PONG response
    client.assert_response(b"+PONG\r\n").await;
}

#[tokio::test]
async fn ping_with_message_non_ascii() {
    let mut client = TestClient::new().await;

    let msg = "Hello, Redis! 🚀";

    // Send PING with message
    client
        .send(format!("*2\r\n$4\r\nPING\r\n${}\r\n{}\r\n", msg.bytes().len(), msg).as_str())
        .await;

    // Read PONG response
    client
        .assert_response(format!("+{}\r\n", msg).as_bytes())
        .await;
}

#[tokio::test]
async fn exists_command() {
    let mut client = TestClient::new().await;

    client.set("key1", "value").await;
    client.set("key2", "value").await;

    client.send_exists(vec!["key1"]).await;
    client.assert_response(b":1\r\n").await;

    client.send_exists(vec!["key42"]).await;
    client.assert_response(b":0\r\n").await;

    client.send_exists(vec!["key1", "key2", "key42"]).await;
    client.assert_response(b":2\r\n").await;
}

#[tokio::test]
async fn db_size_command() {
    let mut client = TestClient::new().await;
    client.assert_size(0).await;

    client.set("key", "value").await;
    client.assert_size(1).await;
}

#[tokio::test]
async fn del_command() {
    let mut client = TestClient::new().await;
    let mut keys = Vec::with_capacity(10);

    for i in 1..=10 {
        keys.push(format!("key{}", i));
        client.set(format!("key{}", i).as_str(), "value").await;
    }

    // Delete one key
    client.send_del(vec!["key10"]).await;
    client.assert_response(b":1\r\n").await;
    keys.pop();

    // 9 keys remaining
    client
        .send_exists(keys.iter().map(|s| s.as_str()).collect())
        .await;
    client.assert_response(b":9\r\n").await;

    // Delete first 5 keys
    client
        .send_del(keys.iter().take(5).map(|s| s.as_str()).collect())
        .await;
    client.assert_response(b":5\r\n").await;

    // 4 keys remaining
    client
        .send_exists(keys.iter().skip(5).map(|s| s.as_str()).collect())
        .await;
    client.assert_response(b":4\r\n").await;

    // Delete the remaining keys
    client
        .send_del(keys.iter().skip(5).map(|s| s.as_str()).collect())
        .await;
    client.assert_response(b":4\r\n").await;

    // No keys remaining
    client.assert_size(0).await;
}

#[tokio::test]
async fn flushdb_command() {
    let mut client = TestClient::new().await;
    let mut keys = Vec::with_capacity(10);

    for i in 1..=10 {
        keys.push(format!("key{}", i));
        client.set(format!("key{}", i).as_str(), "value").await;
    }
    client
        .send_exists(keys.iter().map(|s| s.as_str()).collect())
        .await;
    client.assert_response(b":10\r\n").await;

    // Flush the database
    client.send("*1\r\n$7\r\nFLUSHDB\r\n").await;
    client.assert_response(b"+OK\r\n").await;

    // No keys remaining
    client.assert_size(0).await;
}

#[tokio::test]
async fn unknown_command() {
    let mut client = TestClient::new().await;

    // Send an unknown command
    client.send("*1\r\n$6\r\nFOOBAR\r\n").await;

    // Read the error response
    let expected_err = RedisCommandError::InvalidCommand("FOOBAR".to_string());
    client
        .assert_response(format!("-ERR {}\r\n", expected_err).as_bytes())
        .await;
}

#[tokio::test]
async fn increment_command() {
    let mut client = TestClient::new().await;

    // Increment a non-existing key
    client.send_incr("key42").await;
    client.assert_response(b":1\r\n").await;
    client.assert_size(1).await;

    // // Increment an existing key
    client.send_incr("key42").await;
    client.assert_response(b":2\r\n").await;

    // // Increment a non-existing key
    client.send_incr("key1337").await;
    client.assert_response(b":1\r\n").await;
}

#[tokio::test]
async fn increment_non_integer() {
    env_logger::init();

    let mut client = TestClient::new().await;

    // Set a non-integer value
    client.set("key", "value").await;
    client.send_incr("key").await;
    let expected_err = std::str::from_utf8(b"value")
        .unwrap()
        .parse::<i64>()
        .unwrap_err();
    client
        .assert_response(format!("-ERR {expected_err}\r\n").as_bytes())
        .await;
}

#[tokio::test]
async fn increment_overflow() {
    let mut client = TestClient::new().await;

    let value = i64::MAX - 1;
    client.set("key", value.to_string().as_str()).await;
    client.send_incr("key").await;
    client
        .assert_response(format!(":{}\r\n", value + 1).as_bytes())
        .await;
    client.send_incr("key").await;
    let expected_err = "Integer overflow";
    client
        .assert_response(format!("-ERR {expected_err}\r\n").as_bytes())
        .await;
}
