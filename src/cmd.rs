use std::collections::VecDeque;
use std::time::Duration;

use bytes::Bytes;

use crate::common::bytes_to_string;
use crate::err::RedisCommandError;
use crate::frame::Frame;

#[derive(Debug)]
pub enum Command {
    Get {
        key: String,
    },
    Set {
        key: String,
        val: Bytes,
        expiration: Option<Duration>,
    },
    Ping {
        msg: Option<String>,
    },
    Del {
        keys: Vec<String>,
    },
    Exists {
        keys: Vec<String>,
    },
    Increment {
        key: String,
    },
    Keys {
        pattern: String,
    },
    FlushDB,
    DBSize,
    Unknown(String),
    Lolwut(Vec<Frame>), // Custom command
    Expire {
        key: String,
        seconds: u64,
    },
    #[allow(clippy::upper_case_acronyms)]
    TTL {
        key: String,
    },
}

impl Command {
    pub fn from_frame(frame: Frame) -> anyhow::Result<Command, RedisCommandError> {
        match frame {
            Frame::Array(parts) => {
                if parts.is_empty() {
                    return Err(RedisCommandError::InvalidCommand(
                        "Empty command".to_string(),
                    ));
                }

                // Should be constant time without reallocation
                let mut parts = VecDeque::from(parts);

                // Get the command name
                let command = Self::bulk_to_string(parts.pop_front().unwrap())?;

                match command.to_uppercase().as_str() {
                    "GET" => {
                        if parts.len() != 1 {
                            return Err(Self::wrong_number_of_arguments("GET", "1", parts.len()));
                        }
                        let key = Self::bulk_to_string(parts.pop_front().unwrap())?;
                        Ok(Command::Get { key })
                    }
                    "SET" => {
                        if parts.len() < 2 {
                            return Err(Self::wrong_number_of_arguments(
                                "SET",
                                "2 or 4",
                                parts.len(),
                            ));
                        }
                        let key = Self::bulk_to_string(parts.pop_front().unwrap())?;
                        let val = Self::bulk_to_bytes(parts.pop_front().unwrap())?;
                        if parts.is_empty() {
                            return Ok(Command::Set {
                                key,
                                val,
                                expiration: None,
                            });
                        }
                        if parts.len() != 2 {
                            return Err(Self::wrong_number_of_arguments(
                                "SET",
                                "2 or 4",
                                parts.len(),
                            ));
                        }
                        let expiration = Self::bulk_to_string(parts.pop_front().unwrap())?;
                        match expiration.to_uppercase().as_str() {
                            "PX" => {
                                let px = Self::bulk_to_u64(parts.pop_front().unwrap())?;
                                Ok(Command::Set {
                                    key,
                                    val,
                                    expiration: Some(Duration::from_millis(px)),
                                })
                            }
                            "EX" => {
                                let ex = Self::bulk_to_u64(parts.pop_front().unwrap())?;
                                Ok(Command::Set {
                                    key,
                                    val,
                                    expiration: Some(Duration::from_secs(ex)),
                                })
                            }
                            _ => Err(RedisCommandError::NotImplemented(
                                "Expected EX <seconds> or PX <milliseconds>".to_string(),
                            )),
                        }
                    }
                    "PING" => {
                        if parts.is_empty() {
                            Ok(Command::Ping { msg: None })
                        } else if parts.len() == 1 {
                            let msg = Self::bulk_to_string(parts.pop_front().unwrap())?;
                            Ok(Command::Ping { msg: Some(msg) })
                        } else {
                            return Err(Self::wrong_number_of_arguments(
                                "PING",
                                "0 or 1",
                                parts.len(),
                            ));
                        }
                    }
                    "INCR" => {
                        if parts.len() != 1 {
                            return Err(Self::wrong_number_of_arguments("INCR", "1", parts.len()));
                        }
                        let key = Self::bulk_to_string(parts.pop_front().unwrap())?;
                        Ok(Command::Increment { key })
                    }
                    "FLUSHDB" => {
                        if parts.is_empty() {
                            Ok(Command::FlushDB)
                        } else {
                            Err(Self::wrong_number_of_arguments("FLUSHDB", "0", parts.len()))
                        }
                    }
                    "DBSIZE" => {
                        if parts.is_empty() {
                            Ok(Command::DBSize)
                        } else {
                            Err(Self::wrong_number_of_arguments("DBSIZE", "0", parts.len()))
                        }
                    }
                    "DEL" => {
                        if parts.is_empty() {
                            return Err(Self::wrong_number_of_arguments("DEL", ">0", parts.len()));
                        }
                        let keys = parts
                            .into_iter()
                            .map(Self::bulk_to_string)
                            .collect::<Result<Vec<String>, RedisCommandError>>()?;
                        Ok(Command::Del { keys })
                    }
                    "EXISTS" => {
                        if parts.is_empty() {
                            return Err(Self::wrong_number_of_arguments(
                                "EXISTS",
                                ">0",
                                parts.len(),
                            ));
                        }
                        let keys = parts
                            .into_iter()
                            .map(Self::bulk_to_string)
                            .collect::<Result<Vec<String>, RedisCommandError>>()?;
                        Ok(Command::Exists { keys })
                    }
                    "KEYS" => {
                        if parts.is_empty() {
                            Err(Self::wrong_number_of_arguments("KEYS", "1", parts.len()))
                        } else {
                            let pattern = Self::bulk_to_string(parts.pop_front().unwrap())?;
                            Ok(Command::Keys { pattern })
                        }
                    }
                    "LOLWUT" => {
                        if parts.is_empty() {
                            Err(Self::wrong_number_of_arguments("LOLWUT", "1", parts.len()))
                        } else {
                            Ok(Command::Lolwut(parts.into()))
                        }
                    }
                    "EXPIRE" => {
                        if parts.len() != 2 {
                            return Err(Self::wrong_number_of_arguments(
                                "EXPIRE",
                                "2",
                                parts.len(),
                            ));
                        }
                        let key = Self::bulk_to_string(parts.pop_front().unwrap())?;
                        let seconds = Self::bulk_to_string(parts.pop_front().unwrap())?;
                        let seconds = seconds.parse::<u64>().map_err(|_| {
                            RedisCommandError::ParseDecimalError(format!(
                                "Invalid seconds: {}",
                                seconds
                            ))
                        })?;
                        Ok(Command::Expire { key, seconds })
                    }
                    "TTL" => {
                        if parts.len() != 1 {
                            return Err(Self::wrong_number_of_arguments("TTL", "1", parts.len()));
                        }
                        let key = Self::bulk_to_string(parts.pop_front().unwrap())?;
                        Ok(Command::TTL { key })
                    }
                    _ => Ok(Command::Unknown(command)),
                }
            }
            Frame::Simple(s) if s.to_uppercase() == "PING" => Ok(Command::Ping { msg: None }),
            _ => Err(RedisCommandError::InvalidFrame(
                "Expected array frame".to_string(),
            )),
        }
    }

    fn bulk_to_u64(frame: Frame) -> anyhow::Result<u64, RedisCommandError> {
        match frame {
            Frame::Bulk(bytes) => bytes_to_string(&bytes)?
                .parse::<u64>()
                .map_err(|_| RedisCommandError::ParseIntegerError("Invalid u64".to_string())),
            _ => Err(RedisCommandError::InvalidFrame(
                "Expected bulk string".to_string(),
            )),
        }
    }

    fn wrong_number_of_arguments(
        command: &str,
        expected: &str,
        actual: usize,
    ) -> RedisCommandError {
        RedisCommandError::WrongNumberOfArguments(command.to_string(), expected.to_string(), actual)
    }

    fn bulk_to_string(frame: Frame) -> anyhow::Result<String, RedisCommandError> {
        match frame {
            Frame::Bulk(bytes) => bytes_to_string(&bytes),
            _ => Err(RedisCommandError::InvalidFrame(
                "Expected bulk string".to_string(),
            )),
        }
    }

    fn bulk_to_bytes(frame: Frame) -> anyhow::Result<Bytes, RedisCommandError> {
        match frame {
            Frame::Bulk(bytes) => Ok(bytes),
            _ => Err(RedisCommandError::InvalidFrame(
                "Expected bulk string".to_string(),
            )),
        }
    }
}
