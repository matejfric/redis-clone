use thiserror::Error;

#[derive(Error, Debug)]
pub enum RedisProtocolError {
    #[error("Found `\\n` before `\\r` OR `\\r` without `\\n`")]
    ExcessiveNewline,

    #[error("Invalid frame format. Conversion of `{0}` failed.")]
    ConversionError(String),

    #[error("Unsupported frame type: `{0}_u8`")]
    UnsupportedFrame(u8),

    #[error("Not enough data has been buffered to parse the frame.")]
    NotEnoughData,
}

#[derive(Error, Debug)]
pub enum RedisCommandError {
    #[error("Invalid or unimplemented command: {0}")]
    InvalidCommand(String),

    #[error("Invalid frame format: {0}")]
    InvalidFrame(String),

    #[error("Invalid UTF-8 in command: {0}")]
    InvalidUtf8(String),

    #[error("Wrong number of arguments: {0}")]
    WrongNumberOfArguments(String),
}
