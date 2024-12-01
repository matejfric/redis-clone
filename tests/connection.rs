mod common;

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use redis_clone::{Connection, Frame};
    use tokio::net::TcpStream;

    use super::*;

    /// Test recursive writing of nested arrays.
    #[tokio::test]
    async fn write_nested_array() {
        common::get_or_init_logger();

        let test_server = common::TestServer::new().await;
        let stream = TcpStream::connect(test_server.addr()).await.unwrap();

        let mut conn = Connection::new(stream);

        let command = Frame::Array(vec![
            Frame::Bulk("LOLWUT".into()),
            Frame::Array(vec![
                Frame::Bulk(Bytes::from("Hello, Redis!")),
                Frame::Bulk(Bytes::from("Hello, World!")),
            ]),
            Frame::Array(vec![Frame::Integer(42), Frame::Integer(1337)]),
        ]);
        assert!(conn.write_frame(&command).await.is_ok());

        let expected = Frame::Array(vec![
            Frame::Array(vec![
                Frame::Bulk(Bytes::from("Hello, Redis!")),
                Frame::Bulk(Bytes::from("Hello, World!")),
            ]),
            Frame::Array(vec![Frame::Integer(42), Frame::Integer(1337)]),
            Frame::Simple("https://youtu.be/dQw4w9WgXcQ?si=9GzI0HV44IG4_rPi".to_string()),
        ]);
        let result = conn.read_frame().await.unwrap().unwrap();
        assert_eq!(result, expected);
    }
}
