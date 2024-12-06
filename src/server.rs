use core::str;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use tokio::net::TcpListener;
use tokio::sync::broadcast;
use tokio::time::{timeout, Duration};

use crate::cmd::Command;
use crate::connection::Connection;
use crate::constants::{MAX_CLIENTS, SERVER_SHUTDOWN_CONNECTION_TIMEOUT, TIMEOUT_DURATION};
use crate::db::DB;
use crate::err::RedisCommandError;
use crate::frame::Frame;
use crate::{bulk, error, integer, null, simple};

/// A guard to keep track of the number of active clients.
struct ClientGuard {
    client_count: Arc<AtomicUsize>,
}

impl ClientGuard {
    fn new(client_count: Arc<AtomicUsize>) -> Self {
        client_count.fetch_add(1, Ordering::Relaxed);
        Self { client_count }
    }
}

impl Drop for ClientGuard {
    fn drop(&mut self) {
        self.client_count.fetch_sub(1, Ordering::Relaxed);
    }
}

pub struct RedisServer {
    listener: TcpListener,
    db: DB,
    shutdown: broadcast::Sender<()>,
    handles: Vec<tokio::task::JoinHandle<()>>,
    client_count: Arc<AtomicUsize>,

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
            handles: Vec::new(),
            client_count: Arc::new(AtomicUsize::new(0)),
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
    pub async fn run(&mut self) -> anyhow::Result<()> {
        log::info!(
            "Redis server is running on {}:{}. Ready to accept connections.",
            self.address(),
            self.port()
        );

        let mut shutdown_rx = self.shutdown.subscribe();

        // Setup Ctrl+C signal to shutdown the server.
        let shutdown_handle = self.get_shutdown_handle();
        self.handles.push(tokio::spawn(async move {
            if let Err(e) = tokio::signal::ctrl_c().await {
                log::error!("Failed to listen for Ctrl+C: {}", e);
                return;
            }
            let _ = shutdown_handle.send(());
        }));

        loop {
            let accept = self.accept_connection();
            tokio::select! {
                result = accept => {
                    match result {
                        Ok((mut connection, addr)) => {
                            // Check if the maximum number of clients has been reached.
                            if self.client_count.load(Ordering::Relaxed) >= MAX_CLIENTS {
                                let frame = error!("max number of clients reached");
                                connection.write_frame(&frame).await?;
                                connection.shutdown().await?;
                                log::warn!("Max clients reached, not accepting new connection. Caused by: {}", addr);
                                continue;
                            }

                            let db = self.db.clone();
                            let shutdown_rx = self.shutdown.subscribe();
                            let client_count = Arc::clone(&self.client_count);

                            // Spawn a new task for each connection.
                            self.handles.push(tokio::spawn(async move {
                                match Self::handle_client_connection(connection, db, addr, shutdown_rx, client_count).await {
                                    Ok(_) => log::info!("Closed connection: {}", addr),
                                    Err(e) => log::error!("Connection error for {}: {}", addr, e),
                                };
                            }));
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
        self.shutdown().await
    }

    async fn shutdown(&mut self) -> anyhow::Result<()> {
        // Stop database expiration task
        self.db.shutdown().await?;

        // Stop all active connections
        for handle in self.handles.drain(..) {
            match timeout(SERVER_SHUTDOWN_CONNECTION_TIMEOUT, handle).await {
                Ok(_) => log::debug!("Connection closed"),
                Err(e) => log::error!("Error shutting down connection: {}", e),
            }
        }
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
        client_count: Arc<AtomicUsize>,
    ) -> anyhow::Result<()> {
        let _guard = ClientGuard::new(client_count);
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
                Err(e) => error!(format!("ERR {}", e)),
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
                Some(value) => bulk!(value),
                None => null!(),
            },
            Command::Set {
                key,
                val,
                expiration,
            } => {
                db.set(key, val, expiration).await;
                simple!("OK")
            }
            Command::Ping { msg } => match msg {
                Some(msg) => simple!(msg),
                None => simple!("PONG"),
            },
            Command::Increment { key } => {
                log::debug!("Incrementing key: {}", key);
                match db.increment(&key).await {
                    // Reading bytes from DB should be safe
                    Ok(value) => match unsafe { str::from_utf8_unchecked(&value).parse::<i64>() } {
                        Ok(num) => integer!(num),
                        Err(_) => {
                            error!("ERR value is not an integer or out of range")
                        }
                    },
                    Err(e) => {
                        log::debug!("Error incrementing key: {}", e);
                        error!(format!("ERR {}", e))
                    }
                }
            }
            Command::FlushDB => {
                db.flush().await;
                simple!("OK")
            }
            Command::Del { keys } => {
                let mut count = 0;
                for key in keys {
                    if db.remove(&key).await.is_some() {
                        count += 1;
                    }
                }
                integer!(count)
            }
            Command::Exists { keys } => {
                let mut count = 0;
                for key in keys {
                    if db.exists(key.as_str()).await {
                        count += 1;
                    }
                }
                integer!(count)
            }
            Command::DBSize => integer!(db.size().await as i64),
            Command::Unknown(cmd) => error!(format!(
                "ERR {}",
                RedisCommandError::InvalidCommand(cmd.to_string())
            )),
            Command::Keys { pattern } => {
                let keys = db.keys(pattern.as_str()).await;
                match keys {
                    Ok(keys) => Frame::Array(keys.into_iter().map(|s| bulk!(s)).collect()),
                    Err(e) => error!(format!("ERR {}", e)),
                }
            }
            Command::Lolwut(mut frames) => {
                let mut frames = frames.remove(0);
                match frames.append(simple!("https://youtu.be/dQw4w9WgXcQ?si=9GzI0HV44IG4_rPi")) {
                    Ok(_) => frames,
                    Err(e) => error!(format!("ERR {}", e)),
                }
            }
            Command::Expire { key, seconds } => {
                if db.expire(key.as_str(), Duration::from_secs(seconds)).await {
                    integer!(1)
                } else {
                    integer!(0)
                }
            }
            Command::TTL { key } => {
                let ttl = db.ttl(key.as_str()).await;
                match ttl {
                    Ok(ttl) => match ttl {
                        Some(ttl) => integer!(ttl.as_secs() as i64),
                        None => integer!(-1),
                    },
                    // Key does not exist
                    Err(_) => integer!(-2),
                }
            }
        }
    }
}
