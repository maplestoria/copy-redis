use std::error::Error;
use std::sync::mpsc;
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
                    error!("发送消息失败")
                }
            }
            Object::List(list) => {}
            Object::Set(set) => {}
            Object::SortedSet(sorted_set) => {}
            Object::Hash(hash) => {}
            _ => {}
        }
    }
}

impl Drop for RdbHandlerImpl {
    fn drop(&mut self) {
        if let Err(err) = self.sender.send(Message::Terminate) {
            error!("Closing worker thread error: {}", err)
        }
        // TODO 要等worker线程结束完工作，再退出
    }
}

pub(crate) fn new(target: &str) -> RdbHandlerImpl {
    let addr = target.to_string();
    let (sender, receiver) = mpsc::channel();
    let worker_thread = thread::spawn(move || {
        info!("worker started");
        
        let client = redis::Client::open(addr).unwrap();
        let mut conn = client.get_connection().expect("连接到目的Redis失败");
        let mut pipeline = redis::pipe();
        let mut count = 0;
        let mut timer = Instant::now();
        let hundred_millis = Duration::from_millis(100);
        
        loop {
            match receiver.recv_timeout(Duration::from_millis(1)) {
                Ok(Message::Cmd(cmd)) => {
                    info!("接收到数据");
                    
                    pipeline.add_command(cmd);
                    count += 1;
                }
                Ok(Message::Terminate) => break,
                Err(_) => {}
            }
            let elapsed = timer.elapsed();
            if elapsed.ge(&hundred_millis) && count > 0 {
                match pipeline.query(&mut conn) {
                    Err(err) => {
                        error!("数据写入失败: {}", err.description());
                    }
                    Ok(()) => { info!("写入成功") }
                };
                timer = Instant::now();
                pipeline = redis::pipe();
                count = 0;
            }
        }
        info!("worker ended");
    });
    RdbHandlerImpl {
        worker: Worker { thread: worker_thread },
        sender,
    }
}

struct Worker {
    thread: thread::JoinHandle<()>
}

enum Message {
    Cmd(redis::Cmd),
    Terminate,
}