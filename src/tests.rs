#[cfg(test)]
mod integrate_tests {
    use std::process::Command;
    use std::thread;
    use std::time::Duration;

    use r2d2_redis::redis::RedisResult;
    use redis::Commands;

    use crate::{run, Opt};

    #[test]
    fn test_standalone() {
        let redis_source = start_redis_server(16379);
        let redis_target = start_redis_server(16380);
        let source = "redis://127.0.0.1:16379";
        let target = "redis://127.0.0.1:16380";

        thread::sleep(Duration::from_secs(5));

        let client_s = redis::Client::open(source).unwrap();
        let mut con_s = client_s.get_connection().unwrap();

        let _: () = con_s.set("my_key", 42).unwrap();

        let opt = Opt {
            source: source.to_string(),
            targets: vec![target.to_string()],
            discard_rdb: false,
            aof: false,
            log_file: None,
            sharding: false,
            cluster: false,
            batch_size: 100,
            flush_interval: 100,
            identity: None,
            identity_passwd: None,
        };
        run(opt);

        let client_t = redis::Client::open(target).unwrap();
        let mut con_t = client_t.get_connection().unwrap();

        let result: RedisResult<i32> = redis::cmd("GET").arg("my_key").query(&mut con_t);

        shutdown_redis(redis_source);
        shutdown_redis(redis_target);

        assert_eq!(result, Ok(42));
    }

    #[test]
    fn test_sharding() {
        let redis_source = start_redis_server(16479);
        let redis_target = start_redis_server(16480);
        let redis_target1 = start_redis_server(16481);
        let source = "redis://127.0.0.1:16479";
        let target = "redis://127.0.0.1:16480";
        let target1 = "redis://127.0.0.1:16481";

        thread::sleep(Duration::from_secs(5));

        let client_s = redis::Client::open(source).unwrap();
        let mut con_s = client_s.get_connection().unwrap();

        let _: () = con_s.set("test_sharding", 42).unwrap();

        let opt = Opt {
            source: source.to_string(),
            targets: vec![target.to_string(), target1.to_string()],
            discard_rdb: false,
            aof: false,
            log_file: None,
            sharding: true,
            cluster: false,
            batch_size: 100,
            flush_interval: 100,
            identity: None,
            identity_passwd: None,
        };
        run(opt);

        let client_t = redis::Client::open(target).unwrap();
        let mut con_t = client_t.get_connection().unwrap();
        let result: RedisResult<i32> = redis::cmd("GET").arg("test_sharding").query(&mut con_t);

        let client_t1 = redis::Client::open(target1).unwrap();
        let mut con_t1 = client_t1.get_connection().unwrap();
        let result1: RedisResult<i32> = redis::cmd("GET").arg("test_sharding").query(&mut con_t1);

        shutdown_redis(redis_source);
        shutdown_redis(redis_target);
        shutdown_redis(redis_target1);
        assert!(result.is_ok() || result1.is_ok());
        if result.is_ok() {
            assert_eq!(result, Ok(42));
        }
        if result1.is_ok() {
            assert_eq!(result1, Ok(42));
        }
    }

    fn start_redis_server(port: u16) -> u32 {
        // redis-server --port 6379 --daemonize no --dbfilename rdb --dir ./tests/rdb
        let child = Command::new("redis-server")
            .arg("--port")
            .arg(port.to_string())
            .arg("--daemonize")
            .arg("no")
            .arg("--loglevel")
            .arg("warning")
            .arg("--logfile")
            .arg(port.to_string())
            .spawn()
            .expect("failed to start redis-server");
        return child.id();
    }

    fn shutdown_redis(pid: u32) {
        Command::new("kill")
            .arg("-9")
            .arg(pid.to_string())
            .status()
            .expect("kill redis failed");
    }
}
