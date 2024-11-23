use anyhow::Result;
use atoi::atoi;
use bytes::{Buf, Bytes};
use std::io::Cursor;

use crate::err::RedisProtocolError;

#[derive(Debug)]
pub enum Frame {
    Simple(String), // `+{string data}\r\n`
    Error(String),  // `-{error message}\r\n`
    Integer(i64),   // `:[<+|->]<value>\r\n`
    Bulk(Bytes),    // `${number of bytes}\r\n{data}\r\n`
    Null,           // RESP2: `$-1\r\n (string of length -1)` OR RESP3: `_\r\n`
    Array(Vec<Frame>), // `*{number of elements}\r\n{frames}\r\n`
                    // TODO:
                    //Dictionary(Vec<(String, Frame)>), // `%{number of keys}\r\n{tuples of frame}\r\n`
}

impl Frame {
    pub fn is_parsable(cursor: &mut Cursor<&[u8]>) -> Result<(), RedisProtocolError> {
        if !cursor.has_remaining() {
            // TODO: return a better error
            return Err(RedisProtocolError::MissingNewline.into());
        }
        match cursor.get_u8() {
            b'+' | b'-' | b':' | b'_' => has_newline(cursor),
            b'$' => {
                has_newline(cursor)?; // length
                has_newline(cursor) // bytes
            }
            b'*' => {
                // Array
                let crlf_index = seek_newline(cursor)?;
                let len_u8 = get_byte_slice(cursor, 1, crlf_index);
                let len = atoi::<usize>(len_u8).ok_or_else(|| {
                    RedisProtocolError::ConversionError(String::from_utf8_lossy(len_u8).to_string())
                })?;
                for _ in 0..len {
                    Frame::is_parsable(cursor)?;
                }
                Ok(())
            }
            byte => Err(RedisProtocolError::UnsupportedFrame(byte).into()),
        }
    }

    /// Parse a frame from the buffer. Assumes that the frame was validated by `Frame::is_parsable`.
    pub fn parse(cursor: &mut Cursor<&[u8]>) -> Result<Frame, RedisProtocolError> {
        match cursor.get_u8() {
            b'_' => Ok(Frame::Null),
            b'+' | b'-' => {
                let line = get_line(cursor)?;
                Ok(Frame::Simple(String::from_utf8_lossy(line).to_string()))
            }
            b':' => {
                let line = get_line(cursor)?;
                let num = atoi::<i64>(line).ok_or_else(|| {
                    RedisProtocolError::ConversionError(String::from_utf8_lossy(line).to_string())
                })?;
                Ok(Frame::Integer(num))
            }
            b'$' => {
                let start = cursor.position() as usize;
                let crlf_index = start + seek_newline(cursor)?;
                let len_u8 = get_byte_slice(cursor, start, crlf_index);
                let len = atoi::<i64>(len_u8).ok_or_else(|| {
                    RedisProtocolError::ConversionError(String::from_utf8_lossy(len_u8).to_string())
                })?;

                log::debug!("Parsing bulk string with length: {}", len);

                if len == -1 {
                    return Ok(Frame::Null);
                }

                let data_start = cursor.position() as usize;
                let data_end = data_start + len as usize - 1;

                // Read the data and advance the cursor
                let data = Frame::Bulk(Bytes::copy_from_slice(get_byte_slice(
                    cursor, data_start, data_end,
                )));
                cursor.advance(len as usize + 2);

                Ok(data)
            }
            b'*' => {
                // Example: `echo -e "*3\r\n:-78741\r\n+hello\r\n_\r\n" | nc 127.0.0.1 6379`
                let crlf_index = seek_newline(cursor)?;
                let len_u8 = get_byte_slice(cursor, 1, crlf_index);
                let len = atoi::<usize>(len_u8).ok_or_else(|| {
                    RedisProtocolError::ConversionError(String::from_utf8_lossy(len_u8).to_string())
                })?;
                let mut frames = Vec::with_capacity(len);
                for _ in 0..len {
                    let frame = Frame::parse(cursor)?;
                    frames.push(frame);
                }
                Ok(Frame::Array(frames))
            }
            byte => Err(RedisProtocolError::UnsupportedFrame(byte).into()),
        }
    }
}

/// Returns the index of the first newline character in the buffer
/// (i.e. for `\r\n` return the index of `\r`).
/// The `cursor` is advanced to the next byte after the newline.
fn seek_newline(cursor: &mut Cursor<&[u8]>) -> Result<usize, RedisProtocolError> {
    let mut index = 0;
    while cursor.has_remaining() {
        let byte = cursor.get_u8();
        if byte == b'\r' {
            if cursor.has_remaining() && cursor.get_u8() == b'\n' {
                return Ok(index);
            } else {
                return Err(RedisProtocolError::ExcessiveNewline.into());
            }
        }
        if byte == b'\n' {
            return Err(RedisProtocolError::ExcessiveNewline.into());
        }
        index += 1;
    }
    Err(RedisProtocolError::MissingNewline.into())
}

/// Returns `Ok` if a newline character was found.
/// The `cursor` is advanced to the next byte after the newline.
fn has_newline(cursor: &mut Cursor<&[u8]>) -> Result<(), RedisProtocolError> {
    while cursor.has_remaining() {
        let byte = cursor.get_u8();
        if byte == b'\r' {
            if cursor.has_remaining() && cursor.get_u8() == b'\n' {
                return Ok(());
            } else {
                return Err(RedisProtocolError::ExcessiveNewline.into());
            }
        }
        if byte == b'\n' {
            return Err(RedisProtocolError::ExcessiveNewline.into());
        }
    }
    Err(RedisProtocolError::MissingNewline.into())
}

/// Returns a slice of bytes from `start` to `end` (inclusive).
fn get_byte_slice<'a>(cursor: &Cursor<&'a [u8]>, start: usize, end: usize) -> &'a [u8] {
    &cursor.get_ref()[start..=end]
}

/// Returns a slice of bytes from the current position to the next newline
/// without checking for extra `\n` or `\r` bytes.
fn get_line<'a>(cursor: &mut Cursor<&'a [u8]>) -> Result<&'a [u8], RedisProtocolError> {
    let start = cursor.position() as usize;
    while cursor.has_remaining() {
        let byte = cursor.get_u8();
        if byte == b'\r' {
            if cursor.has_remaining() && cursor.get_u8() == b'\n' {
                // -2 to exclude `\r\n`
                let end = cursor.position() as usize - 2;
                return Ok(&cursor.get_ref()[start..end]);
            }
        }
    }
    Err(RedisProtocolError::MissingNewline.into())
}
