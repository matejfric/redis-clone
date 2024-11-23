use redis_clone::RedisServer;

/// Connect via `redis-cli -h <hostname> -p <port>`
/// (Stop `redis-server` first `sudo systemctl stop redis-server` or choose a custom port.)
///
/// `echo -e '*2\r\n:5\r\n+hello\r\n' | nc 127.0.0.1 6379`
/// `echo -e '*3\r\n$3\r\nSET\r\n$4\r\ntest\r\n$2\r\n42\r\n' | nc 127.0.0.1 6379`
/// `echo -e '*2\r\n$3\r\nGET\r\n$4\r\ntest\r\n' | nc 127.0.0.1 6379`
/// `echo -e '*2\r\n$4\r\nLLEN\r\n$6\r\nmylist\r\n' | nc 127.0.0.1 6379`
///
/// $ RUST_LOG=debug cargo run --bin server
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize the logger.
    env_logger::init();

    let server = RedisServer::new("127.0.0.1", 6379).await?;

    // Setup Ctrl+C signal to shutdown the server.
    let shutdown_handle = server.get_shutdown_handle();
    tokio::spawn(async move {
        if let Err(e) = tokio::signal::ctrl_c().await {
            log::error!("Failed to listen for Ctrl+C: {}", e);
            return;
        }
        let _ = shutdown_handle.send(());
    });

    server.run().await?;

    Ok(())
}
