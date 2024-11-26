# Simple Redis Clone

[![codecov](https://codecov.io/github/matejfric/redis-clone/graph/badge.svg?token=5MVXON09TG)](https://codecov.io/github/matejfric/redis-clone)

## 1. Usage

Start the server with `cargo run`:

```sh
RUST_LOG=info cargo run --bin server
```

Once started, the server can be used manually, for example with `nc`:

```sh
echo -e '*3\r\n$3\r\nSET\r\n$6\r\nanswer\r\n$2\r\n42\r\n' | nc 127.0.0.1 6379
echo -e '*2\r\n$3\r\nGET\r\n$6\r\nanswer\r\n' | nc 127.0.0.1 6379
```

or `redis-cli`:

```sh
redis-cli -h 127.0.0.1 -p 6379
set answer 42
get answer
```

## 2. Notes from Redis Protocol Specification

Sending commands to a Redis server:

1. A client sends the Redis server an *array* consisting of only *bulk strings*.
2. A Redis server replies to clients, sending any valid RESP data type as a reply.

Example:

```txt
C: *2\r\n
C: $4\r\n
C: LLEN\r\n
C: $6\r\n
C: mylist\r\n

S: :42\r\n
```

(The actual interaction is the client sending `*2\r\n$4\r\nLLEN\r\n$6\r\nmylist\r\n` as a whole.)

Simple strings *never* contain carriage return (\r) or line feed (\n) characters. Bulk strings can contain any binary data and may also be referred to as binary or blob.

The difference between simple strings and errors in RESP is that clients should treat errors as exceptions, whereas the string encoded in the error type is the error message itself.

## 3. Progress

- [x] Implement RESP protocol parser and serializer for selected types.
  - [x] Simple strings
  - [x] Errors
  - [x] Integers
  - [x] Bulk strings
  - [x] Arrays
  - [x] Null
  - [ ] Boolean
  - [ ] Map (dictionary)

- [x] Implement selected Redis commands.
  - [x] `PING`
  - [x] `SET`
  - [x] `GET`
  - [x] `DEL`
  - [x] `INCR`
  - [x] `EXISTS`
  - [x] `FLUSHDB`
  - [x] `DBSIZE`
  - [ ] `KEYS`

### 3.1. Optional

- [ ] [Redis pipelining](https://redis.io/docs/latest/develop/use/pipelining/)
- [ ] [Tokio codec](https://docs.rs/tokio-util/latest/tokio_util/codec/index.html)
- [ ] [Sharded DB](https://tokio.rs/tokio/tutorial/shared-state#mutex-sharding)
- [ ] [LOLWUT](https://redis.io/commands/lolwut)

## 4. Testing

Run tests with `cargo test`.
Run tests on a single thread with `cargo test -- --test-threads=1`.
Run a single test with `cargo test --test <test_file> [<test_name>]`.

## 5. Contributing

Please setup pre-commit hooks with the provided script:

```sh
sh setup-precommit-hooks.sh 
```

## 6. Sources

- [Redis protocol specification](https://redis.io/docs/latest/develop/reference/protocol-spec/)
- [Redis commands](https://redis.io/docs/latest/commands/)
- [Tokio tutorial](https://tokio.rs/tokio/tutorial)
