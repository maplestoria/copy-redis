use std::sync::{Arc, mpsc};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::Sender;
use std::thread;
use std::time::{Duration, Instant};

use log::{error, info};
use redis_event::{Event, EventHandler};
use redis_event::cmd::Command;
use redis_event::cmd::strings::{ExistType, ExpireType};
use redis_event::Event::{AOF, RDB};
use redis_event::rdb::Object;

pub(crate) struct EventHandlerImpl {
    worker: Worker,
    sender: Sender<Message>,
    db: isize,
}

impl EventHandler for EventHandlerImpl {
    fn handle(&mut self, event: Event) {
        match event {
            RDB(rdb) => {
                self.handle_rdb(rdb);
            }
            AOF(cmd) => {
                self.handle_aof(cmd);
            }
        }
    }
}

impl EventHandlerImpl {
    fn handle_rdb(&mut self, rdb: Object) {
        match rdb {
            Object::String(kv) => {
                if kv.meta.db != self.db {
                    let mut cmd = redis::cmd("select");
                    cmd.arg(kv.meta.db);
                    self.sender.send(Message::Cmd(cmd)).expect("发送消息失败");
                    self.db = kv.meta.db;
                }
                let mut cmd = redis::cmd("set");
                cmd.arg(kv.key).arg(kv.value);
                self.sender.send(Message::Cmd(cmd)).expect("发送消息失败");
            }
            Object::List(list) => {
                if list.meta.db != self.db {
                    let mut cmd = redis::cmd("select");
                    cmd.arg(list.meta.db);
                    self.sender.send(Message::Cmd(cmd)).expect("发送消息失败");
                    self.db = list.meta.db;
                }
                let mut cmd = redis::cmd("rpush");
                cmd.arg(list.key);
                for val in list.values {
                    cmd.arg(val.as_slice());
                }
                self.sender.send(Message::Cmd(cmd)).expect("发送消息失败");
            }
            Object::Set(set) => {
                if set.meta.db != self.db {
                    let mut cmd = redis::cmd("select");
                    cmd.arg(set.meta.db);
                    self.sender.send(Message::Cmd(cmd)).expect("发送消息失败");
                    self.db = set.meta.db;
                }
                let mut cmd = redis::cmd("sadd");
                cmd.arg(set.key);
                for member in set.members {
                    cmd.arg(member.as_slice());
                }
                self.sender.send(Message::Cmd(cmd)).expect("发送消息失败");
            }
            Object::SortedSet(sorted_set) => {
                if sorted_set.meta.db != self.db {
                    let mut cmd = redis::cmd("select");
                    cmd.arg(sorted_set.meta.db);
                    self.sender.send(Message::Cmd(cmd)).expect("发送消息失败");
                    self.db = sorted_set.meta.db;
                }
                let mut cmd = redis::cmd("zadd");
                cmd.arg(sorted_set.key);
                for item in sorted_set.items {
                    cmd.arg(item.score).arg(item.member.as_slice());
                }
                self.sender.send(Message::Cmd(cmd)).expect("发送消息失败");
            }
            Object::Hash(hash) => {
                if hash.meta.db != self.db {
                    let mut cmd = redis::cmd("select");
                    cmd.arg(hash.meta.db);
                    self.sender.send(Message::Cmd(cmd)).expect("发送消息失败");
                    self.db = hash.meta.db;
                }
                let mut cmd = redis::cmd("hmset");
                cmd.arg(hash.key);
                for field in hash.fields {
                    cmd.arg(field.name.as_slice()).arg(field.value.as_slice());
                }
                self.sender.send(Message::Cmd(cmd)).expect("发送消息失败");
            }
            _ => {}
        }
    }
    
    fn handle_aof(&mut self, cmd: Command) {
        match cmd {
            Command::APPEND(_) => {}
            Command::BITFIELD(_) => {}
            Command::BITOP(_) => {}
            Command::BRPOPLPUSH(_) => {}
            Command::DECR(_) => {}
            Command::DECRBY(_) => {}
            Command::DEL(del) => {
                let mut cmd = redis::cmd("del");
                for key in &del.keys {
                    cmd.arg(key.as_slice());
                }
                self.sender.send(Message::Cmd(cmd)).expect("发送消息失败");
            }
            Command::EVAL(_) => {}
            Command::EVALSHA(_) => {}
            Command::EXPIRE(_) => {}
            Command::EXPIREAT(_) => {}
            Command::EXEC => {}
            Command::FLUSHALL(_) => {}
            Command::FLUSHDB(_) => {}
            Command::GETSET(_) => {}
            Command::HDEL(_) => {}
            Command::HINCRBY(_) => {}
            Command::HMSET(_) => {}
            Command::HSET(_) => {}
            Command::HSETNX(_) => {}
            Command::INCR(_) => {}
            Command::INCRBY(_) => {}
            Command::LINSERT(_) => {}
            Command::LPOP(_) => {}
            Command::LPUSH(_) => {}
            Command::LPUSHX(_) => {}
            Command::LREM(_) => {}
            Command::LSET(_) => {}
            Command::LTRIM(_) => {}
            Command::MOVE(_) => {}
            Command::MSET(_) => {}
            Command::MSETNX(_) => {}
            Command::MULTI => {}
            Command::PERSIST(_) => {}
            Command::PEXPIRE(_) => {}
            Command::PEXPIREAT(_) => {}
            Command::PFADD(_) => {}
            Command::PFCOUNT(_) => {}
            Command::PFMERGE(_) => {}
            Command::PSETEX(_) => {}
            Command::PUBLISH(_) => {}
            Command::RENAME(_) => {}
            Command::RENAMENX(_) => {}
            Command::RESTORE(_) => {}
            Command::RPOP(_) => {}
            Command::RPOPLPUSH(_) => {}
            Command::RPUSH(_) => {}
            Command::RPUSHX(_) => {}
            Command::SADD(_) => {}
            Command::SCRIPTFLUSH => {}
            Command::SCRIPTLOAD(_) => {}
            Command::SDIFFSTORE(_) => {}
            Command::SET(set) => {
                let mut cmd = redis::cmd("set");
                cmd.arg(set.key).arg(set.value);
                if let Some((expire_type, value)) = set.expire.as_ref() {
                    match expire_type {
                        ExpireType::EX => {
                            cmd.arg("EX").arg(value.as_slice());
                        }
                        ExpireType::PX => {
                            cmd.arg("PX").arg(value.as_slice());
                        }
                    }
                }
                if let Some(exist) = set.exist_type.as_ref() {
                    match exist {
                        ExistType::NX => {
                            cmd.arg("NX");
                        }
                        ExistType::XX => {
                            cmd.arg("XX");
                        }
                    }
                }
                self.sender.send(Message::Cmd(cmd)).expect("发送消息失败");
            }
            Command::SETBIT(_) => {}
            Command::SETEX(_) => {}
            Command::SETNX(_) => {}
            Command::SELECT(select) => {
                let mut cmd = redis::cmd("select");
                cmd.arg(select.db);
                self.sender.send(Message::Cmd(cmd)).expect("发送消息失败");
            }
            Command::SETRANGE(_) => {}
            Command::SINTERSTORE(_) => {}
            Command::SMOVE(_) => {}
            Command::SORT(_) => {}
            Command::SREM(_) => {}
            Command::SUNIONSTORE(_) => {}
            Command::SWAPDB(_) => {}
            Command::UNLINK(_) => {}
            Command::ZADD(_) => {}
            Command::ZINCRBY(_) => {}
            Command::ZINTERSTORE(_) => {}
            Command::ZPOPMAX(_) => {}
            Command::ZPOPMIN(_) => {}
            Command::ZREM(_) => {}
            Command::ZREMRANGEBYLEX(_) => {}
            Command::ZREMRANGEBYRANK(_) => {}
            Command::ZREMRANGEBYSCORE(_) => {}
            Command::ZUNIONSTORE(_) => {}
        }
    }
}

impl Drop for EventHandlerImpl {
    fn drop(&mut self) {
        if let Err(_) = self.sender.send(Message::Terminate) {}
        if let Some(thread) = self.worker.thread.take() {
            if let Err(_) = thread.join() {}
        }
    }
}

pub(crate) fn new(target: &str, running: Arc<AtomicBool>) -> EventHandlerImpl {
    let addr = target.to_string();
    let (sender, receiver) = mpsc::channel();
    let worker_thread = thread::spawn(move || {
        info!("Worker thread started");
        let client = redis::Client::open(addr).unwrap();
        let mut conn = client.get_connection().expect("连接到目的Redis失败");
        let mut pipeline = redis::pipe();
        let mut count = 0;
        let mut timer = Instant::now();
        let hundred_millis = Duration::from_millis(100);
        let mut shutdown = false;
        while running.load(Ordering::Relaxed) {
            match receiver.recv_timeout(Duration::from_millis(1)) {
                Ok(Message::Cmd(cmd)) => {
                    pipeline.add_command(cmd);
                    count += 1;
                }
                Ok(Message::Terminate) => {
                    running.store(false, Ordering::Relaxed);
                    shutdown = true;
                }
                Err(_) => {}
            }
            let elapsed = timer.elapsed();
            if (elapsed.ge(&hundred_millis) && count > 0) || shutdown {
                match pipeline.query(&mut conn) {
                    Err(err) => {
                        error!("数据写入失败: {}", err);
                    }
                    Ok(()) => info!("写入成功: {}", count)
                };
                timer = Instant::now();
                pipeline = redis::pipe();
                count = 0;
            }
        }
        info!("Worker thread terminated");
    });
    EventHandlerImpl {
        worker: Worker { thread: Option::Some(worker_thread) },
        sender,
        db: -1,
    }
}

struct Worker {
    thread: Option<thread::JoinHandle<()>>
}

enum Message {
    Cmd(redis::Cmd),
    Terminate,
}