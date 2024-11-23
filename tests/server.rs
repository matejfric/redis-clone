use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::OnceCell;
use tokio::time::sleep;
use tokio::time::timeout;

use redis_clone::RedisServer;

const SERVER_ADDR: &str = "127.0.0.1";
const SERVER_PORT: u16 = 6379;
const CONNECTION_TIMEOUT: Duration = Duration::from_secs(3);
static SERVER_INIT: OnceCell<()> = OnceCell::const_new();

async fn ensure_server_running() {
    SERVER_INIT
        .get_or_init(|| async {
            if let Ok(_) = TcpStream::connect(format!("{}:{}", SERVER_ADDR, SERVER_PORT)).await {
                println!("Server already running at {}:{}", SERVER_ADDR, SERVER_PORT);
                return;
            }
            println!(
                "Starting new server instance at {}:{}",
                SERVER_ADDR, SERVER_PORT
            );

            let server = RedisServer::new(SERVER_ADDR, SERVER_PORT)
                .await
                .expect("Failed to create Redis server.");

            // Spawn the server in a separate thread
            // (tokio::spawn doesn't seem to work in tests there must be a race condition,
            //  having the server turned on before running the tests works fine)

            // tokio::spawn(async move {
            //     server.run().await.unwrap();
            // });

            std::thread::spawn(move || {
                tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap()
                    .block_on(async move {
                        server.run().await.unwrap();
                    });
            });

            // Verify server is running by attempting to connect
            let max_retries = 5;
            let mut retries = 0;
            while retries < max_retries {
                if TcpStream::connect((SERVER_ADDR, SERVER_PORT)).await.is_ok() {
                    return;
                }
                retries += 1;

                // Exponential backoff
                sleep(Duration::from_millis(200 * retries)).await;
            }
            panic!("Server failed to start after {} retries", max_retries);
        })
        .await;
}

struct TestClient {
    stream: TcpStream,
}

impl TestClient {
    async fn new() -> Self {
        ensure_server_running().await;
        let stream = timeout(
            CONNECTION_TIMEOUT,
            TcpStream::connect((SERVER_ADDR, SERVER_PORT)),
        )
        .await
        .expect("Connection timeout")
        .expect("Failed to connect to Redis server");

        TestClient { stream }
    }

    async fn send(&mut self, command: &str) {
        self.stream
            .write_all(command.as_bytes())
            .await
            .expect("Failed to write to server");
        self.stream.flush().await.expect("Failed to flush");
    }

    async fn read_exact(&mut self, n: usize) -> Vec<u8> {
        let mut response = vec![0; n];
        self.stream
            .read_exact(&mut response)
            .await
            .expect("Failed to read from server");
        response
    }
}

#[tokio::test]
async fn get_before_set() {
    let mut client = TestClient::new().await;
    client.send("*2\r\n$3\r\nGET\r\n$5\r\nkey99\r\n").await;
    assert_eq!(b"$-1\r\n", &client.read_exact(5).await[..]);
}

#[tokio::test]
async fn set_and_get_value() {
    let mut client = TestClient::new().await;

    // Set a key
    client
        .send("*3\r\n$3\r\nSET\r\n$5\r\nhello\r\n$5\r\nworld\r\n")
        .await;
    assert_eq!(b"+OK\r\n", &client.read_exact(5).await[..]);

    // Get the key
    client.send("*2\r\n$3\r\nGET\r\n$5\r\nhello\r\n").await;
    assert_eq!(b"$5\r\nworld\r\n", &client.read_exact(11).await[..]);
}

#[tokio::test]
async fn overwrite_key() {
    let mut client = TestClient::new().await;

    // Set a key
    client
        .send("*3\r\n$3\r\nSET\r\n$4\r\nkey1\r\n$4\r\nval1\r\n")
        .await;
    assert_eq!(b"+OK\r\n", &client.read_exact(5).await[..]);

    // Overwrite the key with a new value
    client
        .send("*3\r\n$3\r\nSET\r\n$4\r\nkey1\r\n$4\r\nval2\r\n")
        .await;
    assert_eq!(b"+OK\r\n", &client.read_exact(5).await[..]);

    // Get the key
    client.send("*2\r\n$3\r\nGET\r\n$4\r\nkey1\r\n").await;
    assert_eq!(b"$4\r\nval2\r\n", &client.read_exact(10).await[..]);
}

#[tokio::test]
async fn ping_command() {
    let mut client = TestClient::new().await;

    // Send PING
    client.send("*1\r\n$4\r\nPING\r\n").await;

    // Read PONG response
    assert_eq!(b"+PONG\r\n", &client.read_exact(7).await[..]);
}
