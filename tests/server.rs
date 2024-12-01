use redis_clone::err::RedisCommandError;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::timeout;

use redis_clone::constants::CLIENT_CONNECTION_TIMEOUT;

mod common;

struct TestClient {
    stream: TcpStream,
}

impl TestClient {
    async fn new(server_port: u16) -> Self {
        let stream = timeout(
            CLIENT_CONNECTION_TIMEOUT,
            TcpStream::connect((common::SERVER_ADDR, server_port)),
        )
        .await
        .expect("Connection timeout")
        .unwrap_or_else(|_| {
            panic!(
                "Failed to connect to Redis server {}:{}",
                common::SERVER_ADDR,
                server_port
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

/// Tests without usage of the `frame` and `connection` modules.
/// Should be compliant with the Redis protocol.
#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn get_before_set() {
        common::get_or_init_logger();
        let port = common::TestServer::new().await.port();
        let mut client = TestClient::new(port).await;
        client.send_get("key42").await;
        client.assert_response(b"$-1\r\n").await;
    }

    #[tokio::test]
    async fn set_and_get_value() {
        let port = common::TestServer::new().await.port();
        let mut client = TestClient::new(port).await;
        client.set("hello", "world").await;
        client.send_get("hello").await;
        client.assert_response(b"$5\r\nworld\r\n").await;
    }

    #[tokio::test]
    async fn overwrite_key() {
        let port = common::TestServer::new().await.port();
        let mut client = TestClient::new(port).await;

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
        let port = common::TestServer::new().await.port();
        let mut client = TestClient::new(port).await;

        // Send PING
        client.send("*1\r\n$4\r\nPING\r\n").await;

        // Read PONG response
        client.assert_response(b"+PONG\r\n").await;
    }

    #[tokio::test]
    async fn ping_with_message_non_ascii() {
        let port = common::TestServer::new().await.port();
        let mut client = TestClient::new(port).await;

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
        let port = common::TestServer::new().await.port();
        let mut client = TestClient::new(port).await;

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
        let port = common::TestServer::new().await.port();
        let mut client = TestClient::new(port).await;

        client.assert_size(0).await;

        client.set("key", "value").await;
        client.assert_size(1).await;
    }

    #[tokio::test]
    async fn del_command() {
        let port = common::TestServer::new().await.port();
        let mut client = TestClient::new(port).await;

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
        let port = common::TestServer::new().await.port();
        let mut client = TestClient::new(port).await;

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
        let port = common::TestServer::new().await.port();
        let mut client = TestClient::new(port).await;

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
        common::get_or_init_logger();
        let port = common::TestServer::new().await.port();
        let mut client = TestClient::new(port).await;

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
        let port = common::TestServer::new().await.port();
        let mut client = TestClient::new(port).await;

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
        common::get_or_init_logger();

        let port = common::TestServer::new().await.port();
        let mut client = TestClient::new(port).await;

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

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_clients() {
        common::get_or_init_logger();
        let port = common::TestServer::new().await.port();

        // Number of concurrent clients
        const NUM_CLIENTS: usize = 16;

        // Number of operations per client
        const OPS_PER_CLIENT: usize = 100;

        // Create a barrier to synchronize client start
        let barrier = common::create_barrier(NUM_CLIENTS);

        // Spawn multiple client tasks
        let handles = (0..NUM_CLIENTS)
            .map(|client_id| {
                let barrier_clone = barrier.clone();

                tokio::spawn(async move {
                    // Create a new client for this task
                    let mut client = TestClient::new(port).await;

                    // Wait for all clients to be ready
                    barrier_clone.wait().await;

                    // Perform concurrent operations
                    for i in 0..OPS_PER_CLIENT {
                        let key = format!("key_{}_{}", client_id, i);
                        let value = format!("value_{}_{}", client_id, i);

                        // Perform SET operation
                        client.set(key.as_str(), value.as_str()).await;

                        // Perform GET operation to verify
                        client.send_get(key.as_str()).await;
                        client
                            .assert_response(
                                format!("${}\r\n{}\r\n", value.len(), value).as_bytes(),
                            )
                            .await;
                    }
                })
            })
            .collect::<Vec<_>>();

        // Wait for all client tasks to complete
        for handle in handles {
            handle.await.expect("Client task failed");
        }

        // Verify the final database size
        let mut final_client = TestClient::new(port).await;
        final_client
            .assert_size((NUM_CLIENTS * OPS_PER_CLIENT) as i64)
            .await;
    }
}
