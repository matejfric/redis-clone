use tokio::net::TcpListener;
use tokio::time::{timeout, Duration};

use redis_clone::cmd::Command;
use redis_clone::connection::Connection;
use redis_clone::db::DB;
use redis_clone::err::RedisCommandError;
use redis_clone::frame::Frame;

const TIMEOUT_DURATION: Duration = Duration::from_secs(2);

async fn handle_client_connection(mut conn: Connection, db: DB) -> anyhow::Result<()> {
    loop {
        let frame = match timeout(TIMEOUT_DURATION, conn.read_frame()).await {
            Ok(result) => result?,
            Err(_) => {
                log::warn!(
                    "Client connection timed out after {} miliseconds",
                    TIMEOUT_DURATION.as_millis()
                );
                break Ok(());
            }
        };

        match frame {
            None => {
                // Read 0, closing connection gracefully.
                break Ok(());
            }
            Some(frame) => {
                log::debug!("Received: {:?}", frame);

                let response = match Command::from_frame(frame) {
                    Ok(command) => match command {
                        Command::Get { key } => match db.get(&key) {
                            Some(value) => Frame::Bulk(value),
                            None => Frame::Null,
                        },
                        Command::Set { key, val } => {
                            db.set(key, val);
                            Frame::Simple("OK".to_string())
                        }
                        Command::Ping => Frame::Simple("PONG".to_string()),
                        Command::Unknown(cmd) => Frame::Error(format!(
                            "ERR {}",
                            RedisCommandError::InvalidCommand(cmd.to_string())
                        )),
                    },
                    Err(e) => Frame::Error(format!("ERR {}", e)),
                };

                conn.write_frame(&response).await?;
            }
        }
    }
}

/// Connect via `redis-cli -h <hostname> -p <port>`
/// (Stop `redis-server` first `sudo systemctl stop redis-server` or choose a custom port.)
///
/// `echo -e '*2\r\n:5\r\n+hello\r\n' | nc 127.0.0.1 6379`
/// `echo -e '*3\r\n$3\r\nSET\r\n$4\r\ntest\r\n$2\r\n42\r\n' | nc 127.0.0.1 6379`
/// `echo -e '*2\r\n$3\r\nGET\r\n$4\r\ntest\r\n' | nc 127.0.0.1 6379`
/// `echo -e '*2\r\n$4\r\nLLEN\r\n$6\r\nmylist\r\n' | nc 127.0.0.1 6379`
///
/// $ RUST_LOG=debug cargo run --bin server
#[tokio::main]
async fn main() {
    // Initialize the logger.
    env_logger::init();

    let port = 6379; // Default Redis port
    let ip_address = "127.0.0.1"; // Localhost

    let db = DB::new();

    // TCP socket server, listening for connections.
    // The socket will be closed when the value is dropped.
    let listener = TcpListener::bind(format!("{}:{}", ip_address, port))
        .await
        .expect("TCP bind failed.");

    loop {
        let (stream, address) = listener
            .accept()
            .await
            .expect("Failed to accept TCP connection.");

        log::info!("Accepted connection: {}", address);

        // Clone the handle to the hash map.
        let db = db.clone();

        // Spawn a new task for each connection.
        // Lifetime must be `'static` (i.e, no references to data owned outside of the task).
        // `move` transfers ownership to the task.
        tokio::spawn(async move {
            let connection = Connection::new(stream);
            match handle_client_connection(connection, db).await {
                Ok(_) => log::info!("Closed connection: {}", address),
                Err(e) => log::error!("Connection error: {}", e),
            };
        });
    }
}
