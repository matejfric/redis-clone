use crate::err::RedisCommandError;
use crate::frame::Frame;
use bytes::Bytes;

#[derive(Debug)]
pub enum Command {
    Get { key: String },
    Set { key: String, val: Bytes },
    Ping,
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
                let command = match parts.remove(0) {
                    Frame::Bulk(bytes) => String::from_utf8(bytes.to_vec()).map_err(|_| {
                        RedisCommandError::InvalidUtf8("Invalid command name".to_string())
                    })?,
                    _ => {
                        return Err(RedisCommandError::InvalidFrame(
                            "Expected bulk string for command".to_string(),
                        ))
                    }
                };

                match command.to_uppercase().as_str() {
                    "GET" => {
                        if parts.len() != 1 {
                            return Err(RedisCommandError::WrongNumberOfArguments(format!(
                                "GET expects exactly one argument, got {}",
                                parts.len()
                            )));
                        }
                        let key = match parts.remove(0) {
                            Frame::Bulk(bytes) => {
                                String::from_utf8(bytes.to_vec()).map_err(|_| {
                                    RedisCommandError::InvalidUtf8("Invalid key".to_string())
                                })?
                            }
                            _ => {
                                return Err(RedisCommandError::InvalidFrame(
                                    "Expected bulk string for key".to_string(),
                                ))
                            }
                        };
                        Ok(Command::Get { key })
                    }
                    "SET" => {
                        if parts.len() != 2 {
                            return Err(RedisCommandError::WrongNumberOfArguments(format!(
                                "SET expects exactly two arguments, got {}",
                                parts.len()
                            )));
                        }
                        let key = match parts.remove(0) {
                            Frame::Bulk(bytes) => {
                                String::from_utf8(bytes.to_vec()).map_err(|_| {
                                    RedisCommandError::InvalidUtf8("Invalid key".to_string())
                                })?
                            }
                            _ => {
                                return Err(RedisCommandError::InvalidFrame(
                                    "Expected bulk string for key".to_string(),
                                ))
                            }
                        };
                        let val = match parts.remove(0) {
                            Frame::Bulk(bytes) => bytes,
                            _ => {
                                return Err(RedisCommandError::InvalidFrame(
                                    "Expected bulk string for value".to_string(),
                                ))
                            }
                        };
                        Ok(Command::Set { key, val })
                    }
                    "PING" => Ok(Command::Ping),
                    _ => Ok(Command::Unknown(command)),
                }
            }
            Frame::Simple(s) if s.to_uppercase() == "PING" => Ok(Command::Ping),
            _ => Err(RedisCommandError::InvalidFrame(
                "Expected array frame".to_string(),
            )),
        }
    }
}
