use core::str;
use std::net::SocketAddr;

use bytes::Bytes;
use tokio::net::TcpListener;
use tokio::sync::broadcast;
use tokio::time::{timeout, Duration};

use crate::cmd::Command;
use crate::connection::Connection;
use crate::constants::TIMEOUT_DURATION;
use crate::db::DB;
use crate::err::RedisCommandError;
use crate::frame::Frame;

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

        // Setup Ctrl+C signal to shutdown the server.
        let shutdown_handle = self.get_shutdown_handle();
        tokio::spawn(async move {
            if let Err(e) = tokio::signal::ctrl_c().await {
                log::error!("Failed to listen for Ctrl+C: {}", e);
                return;
            }
            let _ = shutdown_handle.send(());
        });

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

        // Stop database expiration task
        self.db.shutdown().await;

        // Give connections time to close gracefully
        tokio::time::sleep(Duration::from_secs(1)).await;

        Ok(())
    }

    /// Get a handle to the shutdown signal.
    pub fn get_shutdown_handle(&self) -> broadcast::Sender<()> {
        self.shutdown.clone()
    }

    /// Accept incoming connection.
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
                Ok(command) => Self::handle_command(command, &db).await,
                Err(e) => Frame::Error(format!("ERR {}", e)),
            };

            match timeout(TIMEOUT_DURATION, conn.write_frame(&response)).await {
                Ok(result) => match result {
                    Ok(_) => log::debug!("Written to {}: {:?}", addr, response),
                    Err(e) => {
                        log::error!("Error writing to {}: {}", addr, e);
                        break Err(e);
                    }
                },
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

    async fn handle_command(command: Command, db: &DB) -> Frame {
        match command {
            Command::Get { key } => match db.get(&key).await {
                Some(value) => Frame::Bulk(value),
                None => Frame::Null,
            },
            Command::Set { key, val } => {
                db.set(key, val, None).await;
                Frame::Simple("OK".to_string())
            }
            Command::Ping { msg } => match msg {
                Some(msg) => Frame::Simple(msg),
                None => Frame::Simple("PONG".to_string()),
            },
            Command::Increment { key } => {
                log::debug!("Incrementing key: {}", key);
                match db.increment(&key).await {
                    // Reading bytes from DB should be safe
                    Ok(value) => match unsafe { str::from_utf8_unchecked(&value).parse::<i64>() } {
                        Ok(num) => Frame::Integer(num),
                        Err(_) => {
                            Frame::Error("ERR value is not an integer or out of range".to_string())
                        }
                    },
                    Err(e) => {
                        log::debug!("Error incrementing key: {}", e);
                        Frame::Error(format!("ERR {}", e))
                    }
                }
            }
            Command::FlushDB => {
                db.flush().await;
                Frame::Simple("OK".to_string())
            }
            Command::Del { keys } => {
                let mut count = 0;
                for key in keys {
                    if db.remove(&key).await.is_some() {
                        count += 1;
                    }
                }
                Frame::Integer(count)
            }
            Command::Exists { keys } => {
                let mut count = 0;
                for key in keys {
                    if db.exists(key.as_str()).await {
                        count += 1;
                    }
                }
                Frame::Integer(count)
            }
            Command::DBSize => Frame::Integer(db.size().await as i64),
            Command::Unknown(cmd) => Frame::Error(format!(
                "ERR {}",
                RedisCommandError::InvalidCommand(cmd.to_string())
            )),
            Command::Keys { pattern } => {
                let keys = db.keys(pattern.as_str()).await;
                match keys {
                    Ok(keys) => Frame::Array(
                        keys.into_iter()
                            .map(|s| Frame::Bulk(Bytes::from(s)))
                            .collect(),
                    ),
                    Err(e) => Frame::Error(format!("ERR {}", e)),
                }
            }
            Command::Lolwut(frames) => {
                let mut frame = Frame::Array(frames);
                match frame.append(Frame::Simple(
                    "https://youtu.be/dQw4w9WgXcQ?si=9GzI0HV44IG4_rPi".to_string(),
                )) {
                    Ok(_) => frame,
                    Err(e) => Frame::Error(format!("ERR {}", e)),
                }
            }
            Command::Expire { key, seconds } => {
                if db.expire(key.as_str(), Duration::from_secs(seconds)).await {
                    Frame::Integer(1)
                } else {
                    Frame::Integer(0)
                }
            }
        }
    }
}
