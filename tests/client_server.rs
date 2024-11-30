mod common;

#[cfg(test)]
mod tests {
    use super::*;

    use assert_matches::assert_matches;

    use redis_clone::common::bytes_to_i64;
    use redis_clone::frame::Frame;

    #[tokio::test]
    async fn concurrent_increment() {
        common::get_or_init_logger();

        let test_server = common::TestServer::new().await;

        // Number of concurrent clients
        const NUM_CLIENTS: usize = 2;

        // Number of increment operations per client
        const OPS_PER_CLIENT: usize = 5;

        // Create a barrier to synchronize client start
        let barrier = common::create_barrier(NUM_CLIENTS);

        // Shared key for concurrent increments
        let shared_key = "key";

        // Spawn multiple client tasks
        let handles = (0..NUM_CLIENTS)
            .map(|_| {
                let barrier_clone = barrier.clone();
                let test_server_clone = test_server.clone();

                tokio::spawn(async move {
                    // Create a new client for this task
                    let mut client = test_server_clone.create_client().await;

                    // Wait for all clients to be ready
                    barrier_clone.wait().await;

                    // Perform concurrent increment operations
                    for _ in 0..OPS_PER_CLIENT {
                        let response = client.incr(shared_key.to_string()).await.unwrap();
                        assert!(response.is_some());
                        assert_matches!(response.unwrap(), Frame::Integer(_));
                    }
                })
            })
            .collect::<Vec<_>>();

        // Wait for all client tasks to complete
        for handle in handles {
            handle.await.expect("Client task failed");
        }

        // Verify the final value of the shared counter
        let mut final_client = test_server.create_client().await;
        let response = final_client
            .get(shared_key.to_string())
            .await
            .unwrap()
            .unwrap();
        let expected = (NUM_CLIENTS * OPS_PER_CLIENT) as i64;
        match response {
            Frame::Bulk(bytes) => assert_eq!(bytes_to_i64(&bytes).unwrap(), expected),
            frame => panic!("Expected bulk frame. Got: {:?}", frame),
        };
    }
}
