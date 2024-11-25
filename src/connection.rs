use anyhow::{bail, Context, Result};
use bytes::{Buf, BytesMut};
use std::io::Cursor;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufWriter};
use tokio::net::TcpStream;

use crate::err::RedisProtocolError;
use crate::frame::Frame;

/// Client connection to the Redis server. Handles reading and writing frames.
///
/// Inspired by https://tokio.rs/tokio/tutorial/framin
pub struct Connection {
    stream: BufWriter<TcpStream>,
    buffer: BytesMut,
}

impl Connection {
    pub fn new(stream: TcpStream) -> Connection {
        Connection {
            stream: BufWriter::new(stream),
            buffer: BytesMut::with_capacity(1024),
        }
    }

    /// Read a frame from the connection.
    ///
    /// Returns `None` if EOF is reached
    pub async fn read_frame(&mut self) -> Result<Option<Frame>> {
        loop {
            // Attempt to parse a frame from the buffered data. If
            // enough data has been buffered, the frame is
            // returned.
            if let Some(frame) = self.parse_frame()? {
                return Ok(Some(frame));
            }

            // There is not enough buffered data to read a frame.
            // Attempt to read more data from the socket.
            //
            // On success, the number of bytes is returned. `0`
            // indicates "end of stream".
            if 0 == self.stream.read_buf(&mut self.buffer).await? {
                // The remote closed the connection. For this to be
                // a clean shutdown, there should be no data in the
                // read buffer. If there is, this means that the
                // client closed the socket while sending a frame.
                if self.buffer.is_empty() {
                    return Ok(None);
                } else {
                    bail!("Connection reset by client.");
                }
            }
        }
    }

    /// Write a frame to the connection.
    pub async fn write_frame(&mut self, frame: &Frame) -> Result<()> {
        match frame {
            Frame::Array(values) => {
                self.stream.write_u8(b'*').await?;
                self.stream
                    .write_all(values.len().to_string().as_bytes())
                    .await?;
                self.stream.write_all(b"\r\n").await?;
                for value in values {
                    self.write_value(value).await?;
                }
            }
            _ => self.write_value(frame).await?,
        }

        // Ensure that the written data is flushed to the socket.
        self.stream
            .flush()
            .await
            .context("Failed to flush the stream.")
    }

    async fn write_value(&mut self, frame: &Frame) -> Result<()> {
        match frame {
            Frame::Simple(value) => {
                self.stream.write_u8(b'+').await?;
                self.stream.write_all(value.as_bytes()).await?;
                self.stream.write_all(b"\r\n").await?;
            }
            Frame::Error(value) => {
                self.stream.write_u8(b'-').await?;
                self.stream.write_all(value.as_bytes()).await?;
                self.stream.write_all(b"\r\n").await?;
            }
            Frame::Integer(value) => {
                self.stream.write_u8(b':').await?;
                self.stream.write_all(value.to_string().as_bytes()).await?;
                self.stream.write_all(b"\r\n").await?;
            }
            Frame::Bulk(value) => {
                self.stream.write_u8(b'$').await?;
                self.stream
                    .write_all(value.len().to_string().as_bytes())
                    .await?;
                self.stream.write_all(b"\r\n").await?;
                self.stream.write_all(value).await?;
                self.stream.write_all(b"\r\n").await?;
            }
            Frame::Null => {
                self.stream.write_all(b"$-1\r\n").await?;
            }
            Frame::Array(_vec) => unimplemented!("Nested arrays are not supported."),
        }
        Ok(())
    }

    /// Parse a frame from the buffered data.
    pub fn parse_frame(&mut self) -> Result<Option<Frame>> {
        let mut buf = Cursor::new(&self.buffer[..]);

        // Check if enough data has been buffered to parse a single frame.
        // (Without allocations of data structures.)
        match Frame::is_parsable(&mut buf) {
            Ok(()) => {
                // `Frame::is_parsable` advances the cursor to the end of the frame.
                // We use this to discard the read buffer.
                let frame_len = buf.position() as usize;

                // Reset the cursor position.
                buf.set_position(0);

                // If the encoded frame representation is invalid,
                // current connection is terminated (without affecting others).
                let frame = Frame::parse(&mut buf)?;

                // Discard the parsed data from the read buffer.
                self.buffer.advance(frame_len);
                if !self.buffer.is_empty() {
                    // Edge case when the request has a missing `\n` after `\r`.
                    // Not the cleanest way :/
                    self.buffer.clear();
                }

                // Return the parsed frame to the caller.
                Ok(Some(frame))
            }
            Err(RedisProtocolError::NotEnoughData) => {
                // Not enough data has been buffered to parse a frame.
                Ok(None)
            }

            // Other RESP error representing an invalid frame.
            // Connection to the client should be terminated.
            Err(e) => Err(e.into()),
        }
    }
}
