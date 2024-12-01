mod common;

#[cfg(test)]
mod tests {
    use redis_clone::{array, bulk, integer, simple, Connection};
    use tokio::net::TcpStream;

    use super::*;

    /// Test recursive writing of nested arrays.
    #[tokio::test]
    async fn write_nested_array() {
        common::get_or_init_logger();

        let test_server = common::TestServer::new().await;
        let stream = TcpStream::connect(test_server.addr()).await.unwrap();

        let mut conn = Connection::new(stream);

        let command = array!(
            bulk!("LOLWUT"),
            array!(bulk!("Hello, Redis!"), bulk!("Hello, World!")),
            array!(integer!(42), integer!(1337)),
        );
        assert!(conn.write_frame(&command).await.is_ok());

        let expected = array!(
            array!(bulk!("Hello, Redis!"), bulk!("Hello, World!")),
            array!(integer!(42), integer!(1337)),
            simple!("https://youtu.be/dQw4w9WgXcQ?si=9GzI0HV44IG4_rPi"),
        );
        let result = conn.read_frame().await.unwrap().unwrap();
        assert_eq!(result, expected);
    }
}
