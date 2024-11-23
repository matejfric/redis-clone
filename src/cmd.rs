use bytes::Bytes;

use crate::common::bytes_to_string;
use crate::err::RedisCommandError;
use crate::frame::Frame;

#[derive(Debug)]
pub enum Command {
    Get { key: String },
    Set { key: String, val: Bytes },
    Ping { msg: Option<String> },
    Unknown(String),
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
                            return Err(RedisCommandError::WrongNumberOfArguments(format!(
                                "GET expects exactly one argument, got {}",
                                parts.len()
                            )));
                        }
                        let key = Self::bulk_to_string(parts.remove(0))?;
                        Ok(Command::Get { key })
                    }
                    "SET" => {
                        if parts.len() != 2 {
                            return Err(RedisCommandError::WrongNumberOfArguments(format!(
                                "SET expects exactly two arguments, got {}",
                                parts.len()
                            )));
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
                            return Err(RedisCommandError::WrongNumberOfArguments(format!(
                                "PING expects zero or one argument, got {}",
                                parts.len()
                            )));
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

    fn bulk_to_string(frame: Frame) -> anyhow::Result<String, RedisCommandError> {
        match frame {
            Frame::Bulk(bytes) => bytes_to_string(bytes),
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
