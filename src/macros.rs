/// Macro for creating Simple frames
///
/// Example:
/// ```
/// use redis_clone::simple;
///
/// let simple_frame = simple!("Hello World");
/// println!("{:?}", simple_frame);
/// ```
#[macro_export]
macro_rules! simple {
    ($s:expr) => {
        $crate::Frame::Simple($s.to_string())
    };
}

/// Macro for creating Error frames
///
/// Example:
/// ```
/// use redis_clone::error;
///
/// let error_frame = error!("Error message");
/// println!("{:?}", error_frame);
/// ```
#[macro_export]
macro_rules! error {
    ($s:expr) => {
        $crate::Frame::Error($s.to_string())
    };
}

/// Macro for creating Integer frames
///
/// Example:
/// ```
/// use redis_clone::integer;
///     
/// let integer_frame = integer!(42);
/// println!("{:?}", integer_frame);
/// ```
#[macro_export]
macro_rules! integer {
    ($i:expr) => {
        $crate::Frame::Integer($i)
    };
}

/// Macro for creating Bulk frames
///  
/// Example:
/// ```
/// use redis_clone::bulk;
///
/// let bulk_frame = bulk!("Hello, World");
/// println!("{:?}", bulk_frame);
/// ```
#[macro_export]
macro_rules! bulk {
    ($s:expr) => {
        $crate::Frame::Bulk(bytes::Bytes::copy_from_slice($s.as_bytes()))
    };
}

/// Macro for creating Null frames
///
/// Example:
/// ```
/// use redis_clone::null;
///
/// let null_frame = null!();
/// println!("{:?}", null_frame);
/// ```
#[macro_export]
macro_rules! null {
    () => {
        $crate::Frame::Null
    };
}

/// Macro for creating Array frames¨
///
/// Example:
/// ```
/// use redis_clone::{simple, integer, bulk, array};
///
/// let array_frame = array!(
///    simple!("Hello"),
///    integer!(123),
///    bulk!("World")
/// );
/// println!("{:?}", array_frame);
/// ```
#[macro_export]
macro_rules! array {
    ($($frame:expr),* $(,)?) => {
        $crate::Frame::Array(vec![$($frame),*])
    };
}
