use std::net::SocketAddr;
use tokio::net::TcpListener;
use tokio::time::{timeout, Duration};

use crate::cmd::Command;
use crate::connection::Connection;
use crate::db::DB;
use crate::err::RedisCommandError;
use crate::frame::Frame;

const TIMEOUT_DURATION: Duration = Duration::from_secs(5);

pub struct RedisServer {
    listener: TcpListener,
    db: DB,
    address: String,
    port: u16, // default Redis port is 6379
}

impl RedisServer {
    pub async fn new(address: &str, port: u16) -> anyhow::Result<Self> {
        let listener = TcpListener::bind((address, port)).await?;
        let db = DB::new();

        Ok(RedisServer {
            listener,
            db,
            address: address.to_string(),
            port,
        })
    }

    pub fn address(&self) -> &str {
        &self.address
    }

    pub fn port(&self) -> u16 {
        self.port
    }

    /// Start the Redis server and listen for incoming connections.
    pub async fn run(&self) -> anyhow::Result<()> {
        loop {
            let (stream, address) = self.listener.accept().await?;
            log::info!("Accepted connection: {}", address);

            // Each connection owns a handle to the DB.
            let db = self.db.clone();

            // Spawn a new task for each connection.
            tokio::spawn(async move {
                let connection = Connection::new(stream);
                match Self::handle_client_connection(connection, db, address).await {
                    Ok(_) => log::info!("Closed connection: {}", address),
                    Err(e) => log::error!("Connection error for {}: {}", address, e),
                };
            });
        }
    }

    async fn handle_client_connection(
        mut conn: Connection,
        db: DB,
        addr: SocketAddr,
    ) -> anyhow::Result<()> {
        loop {
            let frame = match timeout(TIMEOUT_DURATION, conn.read_frame()).await {
                Ok(result) => result?,
                Err(_) => {
                    log::warn!(
                        "Client {} connection timed out after {} seconds",
                        addr,
                        TIMEOUT_DURATION.as_secs()
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
                    log::debug!("Received frame from {}: {:?}", addr, frame);

                    let response = match Command::from_frame(frame) {
                        Ok(command) => Self::handle_command(command, &db),
                        Err(e) => Frame::Error(format!("ERR {}", e)),
                    };

                    match timeout(TIMEOUT_DURATION, conn.write_frame(&response)).await {
                        Ok(result) => result?,
                        Err(_) => {
                            log::warn!(
                                "Client {} write timed out after {} seconds",
                                addr,
                                TIMEOUT_DURATION.as_secs()
                            );
                            break Ok(());
                        }
                    }
                }
            }
        }
    }

    fn handle_command(command: Command, db: &DB) -> Frame {
        match command {
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
        }
    }
}
