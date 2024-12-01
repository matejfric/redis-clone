mod common;

#[cfg(test)]
mod tests {
    use super::common::get_or_init_logger;

    use bytes::Bytes;
    use std::io::Cursor;

    use redis_clone::err::RedisProtocolError;
    use redis_clone::Frame;

    #[test]
    fn test_simple_string() {
        let data = b"+OK\r\n";
        let mut cursor = Cursor::new(&data[..]);

        assert!(Frame::is_parsable(&mut cursor).is_ok());

        cursor.set_position(0);
        match Frame::parse(&mut cursor).unwrap() {
            Frame::Simple(s) => assert_eq!(s, "OK"),
            _ => panic!("Expected Simple frame"),
        }
    }

    #[test]
    fn test_simple_string_gibberish() {
        // I mean no offense to Chinese and Coptic (an extinct language).
        let data = "+汉语 ϩⲉⲛⲥϩⲁⲓ̈ ⲛ̄ⲥⲁϩ 🚀\r\n".as_bytes();
        let mut cursor = Cursor::new(data);

        assert!(Frame::is_parsable(&mut cursor).is_ok());

        cursor.set_position(0);
        match Frame::parse(&mut cursor).unwrap() {
            Frame::Simple(s) => assert_eq!(s, "汉语 ϩⲉⲛⲥϩⲁⲓ̈ ⲛ̄ⲥⲁϩ 🚀"),
            _ => panic!("Expected Simple frame"),
        }
    }

    #[test]
    fn test_integers() {
        let integers = [i64::MIN, -1, 0, 1, i64::MAX];
        for n in integers {
            let data = format!(":{}\r\n", n);
            let data = data.as_bytes();
            let mut cursor = Cursor::new(data);

            assert!(Frame::is_parsable(&mut cursor).is_ok());

            cursor.set_position(0);
            match Frame::parse(&mut cursor).unwrap() {
                Frame::Integer(parsed_n) => {
                    assert_eq!(parsed_n, n, "Failed to parse integer {}", n)
                }
                _ => panic!("Expected Integer frame"),
            }
        }
    }

    #[test]
    fn test_bulk_string() {
        let data = b"$5\r\nhello\r\n";
        let mut cursor = Cursor::new(&data[..]);

        assert!(Frame::is_parsable(&mut cursor).is_ok());

        cursor.set_position(0);
        match Frame::parse(&mut cursor).unwrap() {
            Frame::Bulk(bytes) => assert_eq!(bytes, Bytes::from("hello")),
            _ => panic!("Expected Bulk frame"),
        }
    }

    #[test]
    fn test_bulk_string_gibberish() {
        let my_string = "汉语 ϩⲉⲛⲥϩⲁⲓ̈ ⲛ̄ⲥⲁϩ 🚀";
        let data = format!("${}\r\n{}\r\n", my_string.bytes().len(), my_string);
        let data = data.as_bytes();
        let mut cursor = Cursor::new(data);

        assert!(Frame::is_parsable(&mut cursor).is_ok());

        cursor.set_position(0);
        match Frame::parse(&mut cursor).unwrap() {
            Frame::Bulk(bytes) => assert_eq!(bytes, Bytes::from("汉语 ϩⲉⲛⲥϩⲁⲓ̈ ⲛ̄ⲥⲁϩ 🚀")),
            _ => panic!("Expected Bulk frame"),
        }
    }

    #[test]
    fn test_null() {
        let data = b"_\r\n";
        let mut cursor = Cursor::new(&data[..]);

        assert!(Frame::is_parsable(&mut cursor).is_ok());

        cursor.set_position(0);
        match Frame::parse(&mut cursor).unwrap() {
            Frame::Null => (),
            _ => panic!("Expected Null frame"),
        }
    }

    #[test]
    fn test_array() {
        let data = b"*3\r\n:-78741\r\n+hello\r\n_\r\n";
        let mut cursor = Cursor::new(&data[..]);

        assert!(Frame::is_parsable(&mut cursor).is_ok());

        cursor.set_position(0);
        match Frame::parse(&mut cursor).unwrap() {
            Frame::Array(frames) => {
                assert_eq!(frames.len(), 3);
                match &frames[0] {
                    Frame::Integer(n) => assert_eq!(*n, -78741),
                    _ => panic!("Expected Integer frame"),
                }
                match &frames[1] {
                    Frame::Simple(s) => assert_eq!(s, "hello"),
                    _ => panic!("Expected Simple frame"),
                }
                match &frames[2] {
                    Frame::Null => (),
                    _ => panic!("Expected Null frame"),
                }
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_error_conditions() {
        // Test incomplete data
        let data = b"+OK"; // missing \r\n
        let mut cursor = Cursor::new(&data[..]);
        assert!(
            matches!(
                Frame::is_parsable(&mut cursor),
                Err(RedisProtocolError::NotEnoughData)
            ),
            "Expected NotEnoughData"
        );

        // Test invalid integer
        let data = b":abc\r\n";
        let mut cursor = Cursor::new(&data[..]);
        cursor.set_position(0);
        assert!(
            matches!(
                Frame::parse(&mut cursor),
                Err(RedisProtocolError::ConversionError(_))
            ),
            "Expected ConversionError"
        );

        // Test unsupported frame type
        let data = b"^123\r\n"; // ^ is not a valid frame type
        let mut cursor = Cursor::new(&data[..]);
        assert!(matches!(
            Frame::parse(&mut cursor),
            Err(RedisProtocolError::UnsupportedFrame(_))
        ));

        // Test excessive newline in simple string
        let data = b"+OK\n0\r\n";
        let mut cursor = Cursor::new(&data[..]);
        assert!(
            matches!(
                Frame::is_parsable(&mut cursor),
                Err(RedisProtocolError::ExcessiveNewline)
            ),
            "Expected ExcessiveNewline error"
        );

        // Test excessive newline in bulk string
        let data = b"$3\r\na\nb\r\n";
        let mut cursor = Cursor::new(&data[..]);
        assert!(Frame::is_parsable(&mut cursor).is_ok(), "Expected Ok");
    }

    #[test]
    fn test_null_bulk_string() {
        get_or_init_logger();

        let data = b"$-1\r\n";
        let mut cursor = Cursor::new(&data[..]);

        assert!(Frame::is_parsable(&mut cursor).is_ok());

        cursor.set_position(0);
        match Frame::parse(&mut cursor).unwrap() {
            Frame::Null => (),
            _ => panic!("Expected Null frame"),
        }
    }

    #[test]
    fn nested_array() {
        get_or_init_logger();

        let data = b"*2\r\n*2\r\n:1\r\n:2\r\n*1\r\n+hello\r\n";
        let mut cursor = Cursor::new(&data[..]);

        let result = Frame::is_parsable(&mut cursor);
        assert!(
            result.is_ok(),
            "Failed to parse frame: {:?}",
            result.err().unwrap()
        );

        cursor.set_position(0);
        match Frame::parse(&mut cursor).unwrap() {
            Frame::Array(frames) => {
                assert_eq!(frames.len(), 2);
                match &frames[0] {
                    Frame::Array(inner_frames) => {
                        assert_eq!(inner_frames.len(), 2);
                        match &inner_frames[0] {
                            Frame::Integer(n) => assert_eq!(*n, 1),
                            _ => panic!("Expected Integer frame"),
                        }
                        match &inner_frames[1] {
                            Frame::Integer(n) => assert_eq!(*n, 2),
                            _ => panic!("Expected Integer frame"),
                        }
                    }
                    _ => panic!("Expected Array frame"),
                }
                match &frames[1] {
                    Frame::Array(inner_frames) => {
                        assert_eq!(inner_frames.len(), 1);
                        match &inner_frames[0] {
                            Frame::Simple(s) => assert_eq!(s, "hello"),
                            _ => panic!("Expected Simple frame"),
                        }
                    }
                    _ => panic!("Expected Array frame"),
                }
            }
            _ => panic!("Expected Array frame"),
        }
    }
}
