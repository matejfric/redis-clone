use core::str;

use bytes::Bytes;

use crate::err::RedisCommandError;

pub fn bytes_to_string(bytes: &Bytes) -> anyhow::Result<String, RedisCommandError> {
    String::from_utf8(bytes.to_vec()).map_err(|e| RedisCommandError::InvalidUtf8(e.to_string()))
}

pub fn bytes_to_i64(bytes: &Bytes) -> anyhow::Result<i64, RedisCommandError> {
    let s = str::from_utf8(bytes)
        .map_err(|e| RedisCommandError::InvalidUtf8(e.to_string()))?
        .trim();
    s.parse::<i64>()
        .map_err(|e| RedisCommandError::ParseDecimalError(e.to_string()))
}
