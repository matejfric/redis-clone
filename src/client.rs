use std::time::Duration;

use anyhow::bail;
use bytes::Bytes;
use tokio::{net::TcpStream, time::timeout};

use crate::cmd::Command;
use crate::connection::Connection;
use crate::constants::CLIENT_CONNECTION_TIMEOUT;
use crate::frame::Frame;

pub struct RedisClient {
    conn: Connection,
}

impl RedisClient {
    /// Create a new Redis client connection
    pub async fn new(address: &str, port: u16) -> anyhow::Result<Self> {
        let stream = timeout(
            CLIENT_CONNECTION_TIMEOUT,
            TcpStream::connect((address, port)),
        )
        .await??;
        let mut conn = Connection::new(stream);

        // TODO: Dirty workaround to check if the server is not full
        // (i.e., reached max client limit).
        // This slows down the connection process and
        // the response may not reach the client in time...
        if let Ok(Ok(Some(Frame::Error(msg)))) =
            timeout(Duration::from_millis(10), conn.read_frame()).await
        {
            log::error!("Error connecting to server: {}", msg);
            conn.shutdown().await?;
            bail!("Error connecting to server: {}", msg)
        }

        Ok(RedisClient { conn })
    }

    /// Send a command and receive a response
    async fn execute(&mut self, command: Command) -> anyhow::Result<Option<Frame>> {
        // Convert command to frame
        let frame = match command {
            Command::Get { key } => Frame::Array(vec![
                Frame::Bulk(Bytes::from("GET")),
                Frame::Bulk(Bytes::from(key)),
            ]),
            Command::Set {
                key,
                val,
                expiration,
            } => {
                let mut cmd = Frame::Array(vec![
                    Frame::Bulk(Bytes::from("SET")),
                    Frame::Bulk(Bytes::from(key)),
                    Frame::Bulk(val),
                ]);
                if expiration.is_some() {
                    cmd.append(Frame::Bulk(Bytes::from("PX")))?;
                    cmd.append(Frame::Bulk(Bytes::from(
                        expiration.unwrap().as_millis().to_string(),
                    )))?;
                }
                cmd
            }
            Command::Ping { msg } => match msg {
                Some(message) => Frame::Array(vec![
                    Frame::Bulk(Bytes::from("PING")),
                    Frame::Bulk(Bytes::from(message)),
                ]),
                None => Frame::Array(vec![Frame::Bulk(Bytes::from("PING"))]),
            },
            Command::Del { keys } => {
                let mut frames = vec![Frame::Bulk(Bytes::from("DEL"))];
                frames.extend(keys.into_iter().map(|key| Frame::Bulk(Bytes::from(key))));
                Frame::Array(frames)
            }
            Command::Exists { keys } => {
                let mut frames = vec![Frame::Bulk(Bytes::from("EXISTS"))];
                frames.extend(keys.into_iter().map(|key| Frame::Bulk(Bytes::from(key))));
                Frame::Array(frames)
            }
            Command::Increment { key } => Frame::Array(vec![
                Frame::Bulk(Bytes::from("INCR")),
                Frame::Bulk(Bytes::from(key)),
            ]),
            Command::FlushDB => Frame::Array(vec![Frame::Bulk(Bytes::from("FLUSHDB"))]),
            Command::DBSize => Frame::Array(vec![Frame::Bulk(Bytes::from("DBSIZE"))]),
            Command::Keys { pattern } => Frame::Array(vec![
                Frame::Bulk(Bytes::from("KEYS")),
                Frame::Bulk(Bytes::from(pattern)),
            ]),
            Command::Unknown(cmd) => Frame::Array(vec![Frame::Bulk(Bytes::from(cmd))]),
            Command::Lolwut(frames) => Frame::Array(vec![
                Frame::Bulk(Bytes::from("LOLWUT")),
                Frame::Array(frames),
            ]),
            Command::Expire { key, seconds } => Frame::Array(vec![
                Frame::Bulk(Bytes::from("EXPIRE")),
                Frame::Bulk(Bytes::from(key)),
                Frame::Bulk(Bytes::from(seconds.to_string())),
            ]),
            Command::TTL { key } => Frame::Array(vec![
                Frame::Bulk(Bytes::from("TTL")),
                Frame::Bulk(Bytes::from(key)),
            ]),
        };

        // Write the frame to the connection
        self.conn.write_frame(&frame).await?;

        // Read the response
        let response = self.conn.read_frame().await?;

        Ok(response)
    }

    /// Ping the Redis server
    pub async fn ping(&mut self, message: Option<String>) -> anyhow::Result<Option<Frame>> {
        let command = Command::Ping { msg: message };
        self.execute(command).await
    }

    /// Get a value by key
    pub async fn get(&mut self, key: String) -> anyhow::Result<Option<Frame>> {
        let command = Command::Get { key };
        self.execute(command).await
    }

    /// Set a key-value pair
    pub async fn set(
        &mut self,
        key: String,
        val: Bytes,
        expiration: Option<Duration>,
    ) -> anyhow::Result<Option<Frame>> {
        let command = Command::Set {
            key,
            val,
            expiration,
        };
        self.execute(command).await
    }

    /// Delete one or more keys
    pub async fn del(&mut self, keys: Vec<String>) -> anyhow::Result<Option<Frame>> {
        let command = Command::Del { keys };
        self.execute(command).await
    }

    /// Check if keys exist
    pub async fn exists(&mut self, keys: Vec<String>) -> anyhow::Result<Option<Frame>> {
        let command = Command::Exists { keys };
        self.execute(command).await
    }

    /// Increment a key
    pub async fn incr(&mut self, key: String) -> anyhow::Result<Option<Frame>> {
        let command = Command::Increment { key };
        self.execute(command).await
    }

    /// Flush the current database
    pub async fn flushdb(&mut self) -> anyhow::Result<Option<Frame>> {
        let command = Command::FlushDB;
        self.execute(command).await
    }

    /// Get the size of the current database
    pub async fn dbsize(&mut self) -> anyhow::Result<Option<Frame>> {
        let command = Command::DBSize;
        self.execute(command).await
    }

    /// Get all keys matching a pattern
    pub async fn keys(&mut self, pattern: String) -> anyhow::Result<Option<Frame>> {
        let command = Command::Keys { pattern };
        self.execute(command).await
    }

    /// Set a key to expire in `seconds`
    ///
    /// Returns 1 if the timeout was set, 0 if the timeout was not set.
    pub async fn expire(&mut self, key: String, seconds: u64) -> anyhow::Result<Option<Frame>> {
        let command = Command::Expire { key, seconds };
        self.execute(command).await
    }

    /// Try to find out
    pub async fn lolwut(&mut self, frames: Vec<Frame>) -> anyhow::Result<Option<Frame>> {
        let command = Command::Lolwut(frames);
        self.execute(command).await
    }

    /// Get the time-to-live for a key
    pub async fn ttl(&mut self, key: String) -> anyhow::Result<Option<Frame>> {
        let command = Command::TTL { key };
        self.execute(command).await
    }
}
