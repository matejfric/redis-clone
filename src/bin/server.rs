use bytes::Bytes;
use std::io;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

use redis_clone::db::DB;

async fn process_tcp_stream(socket: &mut TcpStream, db: DB) -> io::Result<()> {
    let mut buffer = [0; 1024];

    db.insert("apples".to_string(), Bytes::from("10"));

    let apples = db.get("apples").unwrap();

    // Print bytes as string
    println!("Apples: {}", std::str::from_utf8(&apples).unwrap());

    loop {
        // Read data from the client
        let bytes_read = match socket.read(&mut buffer).await {
            Ok(0) => {
                println!("Connection closed by the client.");
                break Ok(());
            }
            Ok(length) => length,
            Err(e) => {
                eprintln!("Failed to read from socket: {}", e);
                break Err(e);
            }
        };

        let request = &buffer[..bytes_read];

        // Process the request line by line
        let lines = request.split(|&c| c == b'\n');
        for line in lines {
            // Convert the line to a string
            let line = match std::str::from_utf8(line) {
                Ok(s) => s,
                Err(_) => {
                    eprintln!("Failed to convert line to UTF-8 string");
                    break;
                }
            };
            println!("Received: {:?}", line);
            match line {
                "PING\\r" => {
                    socket
                        .write_all(b"+PONG\r\n")
                        .await
                        .expect("Failed to write to socket");
                }
                _ => {
                    socket
                        .write_all(b"-ERROR\r\n")
                        .await
                        .expect("Failed to write to socket");
                }
            }
        }
    }
}

#[tokio::main]
async fn main() {
    let port = 6379; // Default Redis port
    let ip_address = "127.0.0.1"; // Localhost

    // let db: DB = Arc::new(Mutex::new(HashMap::new()));
    let db = DB::new();

    // TCP socket server, listening for connections.
    // The socket will be closed when the value is dropped.
    let listener = TcpListener::bind(format!("{}:{}", ip_address, port))
        .await
        .expect("TCP bind failed");

    loop {
        let (mut stream, address) = listener
            .accept()
            .await
            .expect("Failed to accept TCP connection");

        // Clone the handle to the hash map.
        let db = db.clone();

        // Spawn a new task for each connection.
        // Lifetime must be `'static` (i.e, no references to data owned outside of the task).
        // `move` transfers ownership to the task.
        tokio::spawn(async move {
            match process_tcp_stream(&mut stream, db).await {
                Ok(_) => println!("Connection closed: {}", address),
                Err(e) => eprintln!("Connection error: {}", e),
            };
        });
    }
}
