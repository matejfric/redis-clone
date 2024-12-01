use assert_matches::assert_matches;

use redis_clone::common::bytes_to_i64;
use redis_clone::Frame;
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
        assert_eq!(response, Frame::Simple("OK".to_string()));
    }
}

#[cfg(test)]
mod tests {
    use redis_clone::{array, bulk, integer, null, simple};

    use super::*;

    #[tokio::test]
    async fn connection_timeout() {
        // Try connecting to an invalid address with a short timeout.
        let result = RedisClient::new("123.0.0.1", 9999).await;
        // There will be `redis_clone::constants::CLIENT_CONNECTION_TIMEOUT` long delay before timeout.
        assert!(result.is_err(), "Connection should fail");
    }

    #[tokio::test]
    async fn ping() {
        common::get_or_init_logger();

        let test_server = common::TestServer::new().await;
        let mut client = test_server.create_client().await;

        let response = client.ping(None).await.unwrap().unwrap();
        assert_eq!(response, simple!("PONG"));

        let response = client
            .ping(Some("Hello, Redis!".to_string()))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(response, simple!("Hello, Redis!"));
    }

    #[tokio::test]
    async fn del_exists() {
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

        // Check if keys exist
        let response = client
            .exists(keys.iter().map(|s| s.to_string()).collect())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(response, integer!(3));

        // Delete keys
        let response = client
            .del(keys.iter().map(|s| s.to_string()).collect())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(response, integer!(3));

        // Check if keys exist
        for key in &keys {
            let response = client.exists(vec![key.to_string()]).await.unwrap().unwrap();
            assert_eq!(response, integer!(0));
        }
    }

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
    async fn keys() {
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

    #[tokio::test]
    async fn flushdb() {
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

        // Flush the database
        let response = client.flushdb().await.unwrap().unwrap();
        assert_eq!(response, simple!("OK"));

        // Verify that all keys have been removed
        for key in &keys {
            let response = client.get(key.to_string()).await.unwrap().unwrap();
            assert_eq!(response, null!());
        }
    }

    #[tokio::test]
    async fn expire() {
        common::get_or_init_logger();

        let test_server = common::TestServer::new().await;
        let mut client = test_server.create_client().await;

        // Attempt to set expiration on a non-existent key
        let response = client
            .expire("non-existent".to_string(), 1)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(response, integer!(0));

        // Set a key with an expiration
        let key = "key";
        client
            .set(key.to_string(), "value".into())
            .await
            .unwrap()
            .unwrap();
        let response = client.expire(key.to_string(), 1).await.unwrap().unwrap();
        assert_eq!(response, integer!(1));

        // Wait for the key to expire
        tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

        // Verify that the key has been removed
        let response = client.get(key.to_string()).await.unwrap().unwrap();
        assert_eq!(response, null!());

        let response = client.dbsize().await.unwrap().unwrap();
        assert_eq!(response, integer!(0));
    }

    #[tokio::test]
    async fn lolwut() {
        common::get_or_init_logger();

        let test_server = common::TestServer::new().await;
        let mut client = test_server.create_client().await;

        let frames = vec![
            array!(bulk!("Hello, Redis!"), bulk!("Hello, World!")),
            array!(integer!(42), integer!(1337)),
        ];

        let response = client.lolwut(frames).await.unwrap().unwrap();
        assert_matches!(response, Frame::Array(_));

        let expected = array!(
            array!(bulk!("Hello, Redis!"), bulk!("Hello, World!")),
            array!(integer!(42), integer!(1337)),
            simple!("https://youtu.be/dQw4w9WgXcQ?si=9GzI0HV44IG4_rPi"),
        );
        assert_eq!(response, expected);
    }
}
