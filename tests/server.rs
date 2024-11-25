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
        assert_eq!(expected, &response[..]);
    }
}

#[tokio::test]
async fn get_before_set() {
    let mut client = TestClient::new().await;
    client.send("*2\r\n$3\r\nGET\r\n$5\r\nkey99\r\n").await;
    client.assert_response(b"$-1\r\n").await;
}

#[tokio::test]
async fn set_and_get_value() {
    let mut client = TestClient::new().await;

    // Set a key
    client
        .send("*3\r\n$3\r\nSET\r\n$5\r\nhello\r\n$5\r\nworld\r\n")
        .await;
    client.assert_response(b"+OK\r\n").await;

    // Get the key
    client.send("*2\r\n$3\r\nGET\r\n$5\r\nhello\r\n").await;
    client.assert_response(b"$5\r\nworld\r\n").await;
}

#[tokio::test]
async fn overwrite_key() {
    let mut client = TestClient::new().await;

    // Set a key
    client
        .send("*3\r\n$3\r\nSET\r\n$4\r\nkey1\r\n$4\r\nval1\r\n")
        .await;
    client.assert_response(b"+OK\r\n").await;

    // Overwrite the key with a new value
    client
        .send("*3\r\n$3\r\nSET\r\n$4\r\nkey1\r\n$4\r\nval2\r\n")
        .await;
    client.assert_response(b"+OK\r\n").await;

    // Get the key
    client.send("*2\r\n$3\r\nGET\r\n$4\r\nkey1\r\n").await;
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
