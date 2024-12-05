use std::time::Duration;

pub const TIMEOUT_DURATION: Duration = Duration::from_secs(10);
pub const CLIENT_CONNECTION_TIMEOUT: Duration = Duration::from_secs(5);
pub const SERVER_SHUTDOWN_CONNECTION_TIMEOUT: Duration = Duration::from_millis(500);
pub const DB_EXPIRATION_CHECK_INTERVAL: Duration = Duration::from_millis(100);
