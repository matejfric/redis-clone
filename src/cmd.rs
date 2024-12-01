use bytes::Bytes;

use crate::common::bytes_to_string;
use crate::err::RedisCommandError;
use crate::frame::Frame;

#[derive(Debug)]
pub enum Command {
    Get { key: String },
    Set { key: String, val: Bytes },
    Ping { msg: Option<String> },
    Del { keys: Vec<String> },
    Exists { keys: Vec<String> },
    Increment { key: String },
    Keys { pattern: String },
    FlushDB,
    DBSize,
    Unknown(String),
    Lolwut(Vec<Frame>), // Custom command
}

impl Command {
    pub fn from_frame(frame: Frame) -> anyhow::Result<Command, RedisCommandError> {
        match frame {
            Frame::Array(mut parts) => {
                if parts.is_empty() {
                    return Err(RedisCommandError::InvalidCommand(
                        "Empty command".to_string(),
                    ));
                }

                // Get the command name
                let command = Self::bulk_to_string(parts.remove(0))?;

                match command.to_uppercase().as_str() {
                    "GET" => {
                        if parts.len() != 1 {
                            return Err(Self::wrong_number_of_arguments("GET", 1, parts.len()));
                        }
                        let key = Self::bulk_to_string(parts.remove(0))?;
                        Ok(Command::Get { key })
                    }
                    "SET" => {
                        if parts.len() != 2 {
                            return Err(Self::wrong_number_of_arguments("SET", 2, parts.len()));
                        }
                        let key = Self::bulk_to_string(parts.remove(0))?;
                        let val = Self::bulk_to_bytes(parts.remove(0))?;
                        Ok(Command::Set { key, val })
                    }
                    "PING" => {
                        if parts.is_empty() {
                            Ok(Command::Ping { msg: None })
                        } else if parts.len() == 1 {
                            let msg = Self::bulk_to_string(parts.remove(0))?;
                            Ok(Command::Ping { msg: Some(msg) })
                        } else {
                            // TODO: variable number of arguments
                            return Err(Self::wrong_number_of_arguments("PING", 1, parts.len()));
                        }
                    }
                    "INCR" => {
                        if parts.len() != 1 {
                            return Err(Self::wrong_number_of_arguments("INCR", 1, parts.len()));
                        }
                        let key = Self::bulk_to_string(parts.remove(0))?;
                        Ok(Command::Increment { key })
                    }
                    "FLUSHDB" => {
                        if parts.is_empty() {
                            Ok(Command::FlushDB)
                        } else {
                            Err(Self::wrong_number_of_arguments("FLUSHDB", 0, parts.len()))
                        }
                    }
                    "DBSIZE" => {
                        if parts.is_empty() {
                            Ok(Command::DBSize)
                        } else {
                            Err(Self::wrong_number_of_arguments("DBSIZE", 0, parts.len()))
                        }
                    }
                    "DEL" => {
                        if parts.is_empty() {
                            // TODO: >1 arguments
                            return Err(Self::wrong_number_of_arguments("DEL", 1, parts.len()));
                        }
                        let keys = parts
                            .into_iter()
                            .map(Self::bulk_to_string)
                            .collect::<Result<Vec<String>, RedisCommandError>>()?;
                        Ok(Command::Del { keys })
                    }
                    "EXISTS" => {
                        if parts.is_empty() {
                            // TODO: >1 arguments
                            return Err(Self::wrong_number_of_arguments("EXISTS", 1, parts.len()));
                        }
                        let keys = parts
                            .into_iter()
                            .map(Self::bulk_to_string)
                            .collect::<Result<Vec<String>, RedisCommandError>>()?;
                        Ok(Command::Exists { keys })
                    }
                    "KEYS" => {
                        if parts.is_empty() {
                            Err(Self::wrong_number_of_arguments("KEYS", 1, parts.len()))
                        } else {
                            let pattern = Self::bulk_to_string(parts.remove(0))?;
                            Ok(Command::Keys { pattern })
                        }
                    }
                    "LOLWUT" => {
                        if parts.is_empty() {
                            Err(Self::wrong_number_of_arguments("LOLWUT", 1, parts.len()))
                        } else {
                            Ok(Command::Lolwut(parts))
                        }
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

    fn wrong_number_of_arguments(
        command: &str,
        expected: usize,
        actual: usize,
    ) -> RedisCommandError {
        RedisCommandError::WrongNumberOfArguments(command.to_string(), expected, actual)
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
