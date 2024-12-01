use assert_matches::assert_matches;

use redis_clone::common::bytes_to_i64;
use redis_clone::frame::Frame;
use redis_clone::RedisClient;

mod common;

pub trait TestClient {
    #[allow(async_fn_in_trait)]
    async fn set_key_value(&mut self, key: String, value: String);
}

impl TestClient for RedisClient {
    /// Set a key to a value
    async fn set_key_value(&mut self, key: String, value: String) {
        let response = self
            .set(key.to_string(), value.into())
            .await
            .unwrap()
            .unwrap();
        assert_matches!(response, Frame::Simple(_));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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

    #[tokio::test]
    async fn keys_command() {
        common::get_or_init_logger();

        let test_server = common::TestServer::new().await;
        let mut client = test_server.create_client().await;

        // Set some keys
        let keys = vec!["key1", "key2", "key3"];
        for key in &keys {
            client
                .set_key_value(key.to_string(), "value".to_string())
                .await;
        }

        // Get all keys
        let response = client.keys("*".to_string()).await.unwrap().unwrap();
        match response {
            Frame::Array(frames) => {
                let actual_keys = frames
                    .iter()
                    .map(|frame| match frame {
                        Frame::Bulk(bytes) => String::from_utf8(bytes.to_vec()).unwrap(),
                        frame => panic!("Expected bulk frame. Got: {:?}", frame),
                    })
                    .collect::<Vec<_>>();
                assert_eq!(actual_keys.len(), keys.len());
                for key in &keys {
                    assert!(actual_keys.contains(&key.to_string()));
                }
            }
            frame => panic!("Expected array frame. Got: {:?}", frame),
        }
    }

    #[tokio::test]
    async fn keys_with_pattern() {
        common::get_or_init_logger();

        let test_server = common::TestServer::new().await;
        let mut client = test_server.create_client().await;

        // Set some keys
        let keys_to_match = vec!["k1y", "k2y", "k3y"];
        let other_keys = vec!["foo", "bar", "foobar"];
        for key in keys_to_match.iter().chain(&other_keys) {
            client
                .set_key_value(key.to_string(), "value".to_string())
                .await;
        }

        // Get keys matching a pattern
        let response = client.keys("k?y".to_string()).await.unwrap().unwrap();
        match response {
            Frame::Array(frames) => {
                let actual_keys = frames
                    .iter()
                    .map(|frame| match frame {
                        Frame::Bulk(bytes) => String::from_utf8(bytes.to_vec()).unwrap(),
                        frame => panic!("Expected bulk frame. Got: {:?}", frame),
                    })
                    .collect::<Vec<_>>();
                assert_eq!(actual_keys.len(), keys_to_match.len());
                for key in &keys_to_match {
                    assert!(actual_keys.contains(&key.to_string()));
                }
            }
            frame => panic!("Expected array frame. Got: {:?}", frame),
        }
    }
}