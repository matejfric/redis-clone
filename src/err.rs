use thiserror::Error;

#[derive(Error, Debug)]
pub enum RedisProtocolError {
    #[error("Found `\\n` before `\\r` OR `\\r` without `\\n`")]
    ExcessiveNewline,
    #[error("Failed to find newline")]
    MissingNewline,
    #[error("Invalid frame format. Conversion of `{0}` failed.")]
    ConversionError(String),
    #[error("Unsupported frame type: `{0}`")]
    UnsupportedFrame(u8),
}
