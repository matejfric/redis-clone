use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

const SERVER_ADDR: &str = "127.0.0.1:6379";

#[tokio::test]
async fn get_before_set() {
    let mut stream = TcpStream::connect(SERVER_ADDR).await.unwrap();

    // Attempt to GET a key before setting it
    stream
        .write_all(b"*2\r\n$3\r\nGET\r\n$5\r\nkey99\r\n")
        .await
        .unwrap();

    // Read None response
    let mut response = [0; 5];
    stream.read_exact(&mut response).await.unwrap();
    assert_eq!(b"$-1\r\n", &response);
}

#[tokio::test]
async fn set_and_get_value() {
    let mut stream = TcpStream::connect(SERVER_ADDR).await.unwrap();

    // Set a key
    stream
        .write_all(b"*3\r\n$3\r\nSET\r\n$5\r\nhello\r\n$5\r\nworld\r\n")
        .await
        .unwrap();

    // Read OK response
    let mut response = [0; 5];
    stream.read_exact(&mut response).await.unwrap();
    assert_eq!(b"+OK\r\n", &response);

    // Get the key
    stream
        .write_all(b"*2\r\n$3\r\nGET\r\n$5\r\nhello\r\n")
        .await
        .unwrap();

    // Read the value
    let mut response = [0; 11];
    stream.read_exact(&mut response).await.unwrap();
    assert_eq!(b"$5\r\nworld\r\n", &response);
}

#[tokio::test]
async fn overwrite_key() {
    let mut stream = TcpStream::connect(SERVER_ADDR).await.unwrap();

    // Set a key
    stream
        .write_all(b"*3\r\n$3\r\nSET\r\n$4\r\nkey1\r\n$4\r\nval1\r\n")
        .await
        .unwrap();
    let mut response = [0; 5];
    stream.read_exact(&mut response).await.unwrap();
    assert_eq!(b"+OK\r\n", &response);

    // Overwrite the key with a new value
    stream
        .write_all(b"*3\r\n$3\r\nSET\r\n$4\r\nkey1\r\n$4\r\nval2\r\n")
        .await
        .unwrap();
    stream.read_exact(&mut response).await.unwrap();
    assert_eq!(b"+OK\r\n", &response);

    // Get the key
    stream
        .write_all(b"*2\r\n$3\r\nGET\r\n$4\r\nkey1\r\n")
        .await
        .unwrap();
    let mut response = [0; 10];
    stream.read_exact(&mut response).await.unwrap();
    assert_eq!(b"$4\r\nval2\r\n", &response);
}

#[tokio::test]
async fn ping_command() {
    let mut stream = TcpStream::connect(SERVER_ADDR).await.unwrap();

    // Send PING
    stream.write_all(b"*1\r\n$4\r\nPING\r\n").await.unwrap();

    // Read PONG response
    let mut response = [0; 7];
    stream.read_exact(&mut response).await.unwrap();
    assert_eq!(b"+PONG\r\n", &response);
}
