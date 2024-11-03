use anyhow::Result;
use tokio::net::TcpListener;

use redis_clone::connection::Connection;
use redis_clone::db::DB;

// // TODO:
// db.insert("apples".to_string(), Bytes::from("10"));
// let apples = db.get("apples").unwrap();
// // Print bytes as string
// println!("Apples: {}", std::str::from_utf8(&apples).unwrap());

async fn handle_client_connection(mut conn: Connection, db: DB) -> Result<()> {
    loop {
        // Read data from the client
        let frame = conn.read_frame().await?;
        match frame {
            None => break Ok(()),
            Some(frame) => {
                println!("Received: {:?}", frame);
            }
        }
    }
}

/// Connect via `redis-cli -h <hostname> -p <port>`
/// (Stop redis-server first `sudo systemctl stop redis-server` or choose a custom port.)
/// `echo -e "*2\r\n:5\r\n+hello\r\n" | nc 127.0.0.1 6379`
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
        .expect("TCP bind failed.");

    loop {
        let (stream, address) = listener
            .accept()
            .await
            .expect("Failed to accept TCP connection.");

        // Clone the handle to the hash map.
        let db = db.clone();

        // Spawn a new task for each connection.
        // Lifetime must be `'static` (i.e, no references to data owned outside of the task).
        // `move` transfers ownership to the task.
        tokio::spawn(async move {
            let connection = Connection::new(stream);
            match handle_client_connection(connection, db).await {
                Ok(_) => println!("Connection closed: {}", address),
                Err(e) => eprintln!("Connection error: {}", e),
            };
        });
    }
}
