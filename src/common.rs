use bytes::Bytes;

use crate::err::RedisCommandError;

pub fn bytes_to_string(bytes: Bytes) -> anyhow::Result<String, RedisCommandError> {
    String::from_utf8(bytes.to_vec()).map_err(|e| RedisCommandError::InvalidUtf8(e.to_string()))
}
