use std::net::SocketAddr;

use tokio::net::TcpListener;
use tokio::sync::broadcast;
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
    shutdown: broadcast::Sender<()>,

    address: String,
    port: u16, // default Redis port is 6379
}

impl RedisServer {
    pub async fn new(address: &str, port: u16) -> anyhow::Result<Self> {
        let listener = TcpListener::bind((address, port)).await?;
        let db = DB::new();
        let (shutdown, _) = broadcast::channel(1);

        Ok(RedisServer {
            listener,
            db,
            shutdown,
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
        log::info!(
            "Redis server is running on {}:{}. Ready to accept connections.",
            self.address(),
            self.port()
        );

        let mut shutdown_rx = self.shutdown.subscribe();

        loop {
            let accept = self.accept_connection();
            tokio::select! {
                result = accept => {
                    match result {
                        Ok((connection, addr)) => {
                            let db = self.db.clone();
                            let shutdown_rx = self.shutdown.subscribe();

                            // Spawn a new task for each connection.
                            tokio::spawn(async move {
                                match Self::handle_client_connection(connection, db, addr, shutdown_rx).await {
                                    Ok(_) => log::info!("Closed connection: {}", addr),
                                    Err(e) => log::error!("Connection error for {}: {}", addr, e),
                                };
                            });
                        }
                        Err(e) => {
                            log::error!("Accept error: {}", e);
                            break;
                        }
                    }
                }
                _ = shutdown_rx.recv() => {
                    log::info!("Received shutdown signal, stopping server...");
                    break;
                }
            }
        }

        // Give connections time to close gracefully
        tokio::time::sleep(Duration::from_secs(1)).await;

        Ok(())
    }

    /// Get a handle to the shutdown signal.
    pub fn get_shutdown_handle(&self) -> broadcast::Sender<()> {
        self.shutdown.clone()
    }

    async fn accept_connection(&self) -> anyhow::Result<(Connection, SocketAddr)> {
        let (socket, addr) = self.listener.accept().await?;
        log::info!("Accepted connection from: {}", addr);
        Ok((Connection::new(socket), addr))
    }

    async fn handle_client_connection(
        mut conn: Connection,
        db: DB,
        addr: SocketAddr,
        mut shutdown_rx: broadcast::Receiver<()>,
    ) -> anyhow::Result<()> {
        loop {
            let frame = tokio::select! {
                result = timeout(TIMEOUT_DURATION, conn.read_frame()) => {
                    match result {
                        Ok(frame_result) => {
                            match frame_result? {
                                Some(frame) => frame,
                                None => break Ok(()),
                            }
                        }
                        Err(_) => {
                            log::warn!("Client {} connection timed out after {} seconds",
                                addr, TIMEOUT_DURATION.as_secs());
                            break Ok(());
                        }
                    }
                }
                _ = shutdown_rx.recv() => {
                    log::info!("Shutdown signal received, closing connection: {}", addr);
                    break Ok(());
                }
            };

            log::debug!("Received from {}: {:?}", addr, frame);

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
