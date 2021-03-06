use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::Sender;
use std::sync::{mpsc, Arc};
use std::thread;
use std::time::Duration;

use log::{error, info};
use r2d2_redis::redis::cluster::ClusterClient;
use redis::Cmd;
use redis_event::cmd::Command;
use redis_event::{Event, EventHandler};

use crate::command::CommandConverter;
use crate::worker::{Message, Worker};

pub(crate) struct ClusterEventHandlerImpl {
    worker: Worker,
    sender: Sender<Message>,
}

impl EventHandler for ClusterEventHandlerImpl {
    fn handle(&mut self, event: Event) {
        match event {
            Event::RDB(rdb) => self.handle_rdb(rdb),
            Event::AOF(aof) => match aof {
                Command::DEL(del) => {
                    for key in &del.keys {
                        let mut cmd = redis::cmd("DEL");
                        cmd.arg(key.as_slice());
                        self.execute(cmd, None);
                    }
                }
                Command::MSET(mset) => {
                    for kv in &mset.key_values {
                        let mut cmd = redis::cmd("SET");
                        cmd.arg(kv.key).arg(kv.value);
                        self.execute(cmd, None);
                    }
                }
                Command::MSETNX(msetnx) => {
                    for kv in &msetnx.key_values {
                        let mut cmd = redis::cmd("SETNX");
                        cmd.arg(kv.key).arg(kv.value);
                        self.execute(cmd, None);
                    }
                }
                Command::PFCOUNT(pfcount) => {
                    for key in &pfcount.keys {
                        let mut cmd = redis::cmd("PFCOUNT");
                        cmd.arg(*key);
                        self.execute(cmd, None);
                    }
                }
                Command::UNLINK(unlink) => {
                    for key in &unlink.keys {
                        let mut cmd = redis::cmd("UNLINK");
                        cmd.arg(*key);
                        self.execute(cmd, None);
                    }
                }
                Command::BITOP(_)
                | Command::EVAL(_)
                | Command::EVALSHA(_)
                | Command::MULTI
                | Command::EXEC
                | Command::PFMERGE(_)
                | Command::SDIFFSTORE(_)
                | Command::SINTERSTORE(_)
                | Command::SUNIONSTORE(_)
                | Command::ZUNIONSTORE(_)
                | Command::ZINTERSTORE(_)
                | Command::PUBLISH(_) => {}
                _ => self.handle_aof(aof),
            },
        };
    }
}

impl Drop for ClusterEventHandlerImpl {
    fn drop(&mut self) {
        if let Err(_) = self.sender.send(Message::Terminate) {}
        if let Some(thread) = self.worker.thread.take() {
            if let Err(_) = thread.join() {}
        }
    }
}

impl CommandConverter for ClusterEventHandlerImpl {
    fn execute(&mut self, cmd: Cmd, _: Option<&[u8]>) {
        if let Err(err) = self.sender.send(Message::Cmd(cmd)) {
            panic!("{}", err)
        }
    }

    fn swap_db(&mut self, _: i32) {}
}

pub(crate) fn new_cluster(target: Vec<String>, running: Arc<AtomicBool>) -> ClusterEventHandlerImpl {
    let (sender, receiver) = mpsc::channel();
    let worker_thread = thread::spawn(move || {
        info!(target: "cluster::worker", "Worker thread started");
        let mut shutdown = false;
        let client = match ClusterClient::open(target) {
            Ok(client) => client,
            Err(err) => {
                running.store(false, Ordering::SeqCst);
                panic!(err);
            }
        };
        let mut conn = client.get_connection().expect("获取ClusterConnection失败");
        loop {
            match receiver.recv_timeout(Duration::from_millis(10)) {
                Ok(Message::Cmd(cmd)) => {
                    match cmd.query(&mut conn) {
                        Err(err) => {
                            error!(target: "cluster::worker", "数据写入失败: {}", err);
                        }
                        Ok(()) => {}
                    };
                }
                Ok(Message::Terminate) => {
                    shutdown = true;
                }
                _ => {}
            }
            if shutdown {
                break;
            };
        }
        info!(target: "cluster::worker", "Worker thread terminated");
    });
    ClusterEventHandlerImpl {
        worker: Worker {
            thread: Option::Some(worker_thread),
        },
        sender,
    }
}
