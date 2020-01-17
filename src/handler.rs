use std::sync::{Arc, mpsc};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::Sender;
use std::thread;
use std::time::{Duration, Instant};

use log::{error, info};
use redis_event::rdb::Object;
use redis_event::RdbHandler;

pub(crate) struct RdbHandlerImpl {
    worker: Worker,
    sender: Sender<Message>,
}

impl RdbHandler for RdbHandlerImpl {
    fn handle(&mut self, data: Object) {
        match data {
            Object::String(kv) => {
                let mut cmd = redis::cmd("set");
                cmd.arg(kv.key).arg(kv.value);
                if let Err(err) = self.sender.send(Message::Cmd(cmd)) {
                    error!("发送消息失败:{}", err);
                }
            }
            Object::List(list) => {
                let mut cmd = redis::cmd("rpush");
                cmd.arg(list.key);
                for val in list.values {
                    cmd.arg(val.as_slice());
                }
                if let Err(err) = self.sender.send(Message::Cmd(cmd)) {
                    error!("发送消息失败:{}", err);
                }
            }
            Object::Set(set) => {
                let mut cmd = redis::cmd("sadd");
                cmd.arg(set.key);
                for member in set.members {
                    cmd.arg(member.as_slice());
                }
                if let Err(err) = self.sender.send(Message::Cmd(cmd)) {
                    error!("发送消息失败:{}", err);
                }
            }
            Object::SortedSet(sorted_set) => {
                let mut cmd = redis::cmd("zadd");
                cmd.arg(sorted_set.key);
                for item in sorted_set.items {
                    cmd.arg(item.score).arg(item.member.as_slice());
                }
                if let Err(err) = self.sender.send(Message::Cmd(cmd)) {
                    error!("发送消息失败:{}", err);
                }
            }
            Object::Hash(hash) => {
                let mut cmd = redis::cmd("hmset");
                cmd.arg(hash.key);
                for field in hash.fields {
                    cmd.arg(field.name.as_slice()).arg(field.value.as_slice());
                }
                if let Err(err) = self.sender.send(Message::Cmd(cmd)) {
                    error!("发送消息失败:{}", err);
                }
            }
            _ => {}
        }
    }
}

impl Drop for RdbHandlerImpl {
    fn drop(&mut self) {
        if let Err(_) = self.sender.send(Message::Terminate) {}
        if let Some(thread) = self.worker.thread.take() {
            if let Err(_) = thread.join() {}
        }
    }
}

pub(crate) fn new(target: &str, running: Arc<AtomicBool>) -> RdbHandlerImpl {
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
        
        while running.load(Ordering::Relaxed) {
            match receiver.recv_timeout(Duration::from_millis(1)) {
                Ok(Message::Cmd(cmd)) => {
                    pipeline.add_command(cmd);
                    count += 1;
                }
                Ok(Message::Terminate) => running.store(false, Ordering::Relaxed),
                Err(_) => {}
            }
            let elapsed = timer.elapsed();
            if elapsed.ge(&hundred_millis) && count > 0 {
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
    RdbHandlerImpl {
        worker: Worker { thread: Option::Some(worker_thread) },
        sender,
    }
}

struct Worker {
    thread: Option<thread::JoinHandle<()>>
}

enum Message {
    Cmd(redis::Cmd),
    Terminate,
}