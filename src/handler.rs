use std::{io, thread};
use std::sync::{Arc, mpsc};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::Sender;
use std::time::{Duration, Instant};

use log::info;
use redis::cluster::ClusterClient;
use redis::Connection;
use redis_event::{Event, EventHandler};
use redis_event::Event::{AOF, RDB};

use crate::command::CommandConverter;
use crate::sharding::ShardedClient;

pub(crate) struct EventHandlerImpl {
    worker: Worker,
    sender: Sender<Message>,
}

impl EventHandler for EventHandlerImpl {
    fn handle(&mut self, event: Event) {
        let cmd = match event {
            RDB(rdb) => {
                self.handle_rdb(rdb)
            }
            AOF(cmd) => {
                self.handle_aof(cmd)
            }
        };
        if let Some(cmd) = cmd {
            if let Err(err) = self.sender.send(Message::Cmd(cmd)) {
                panic!("{}", err)
            }
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

pub(crate) fn new_sharded(target: Vec<String>, running: Arc<AtomicBool>) -> EventHandlerImpl {
    let (sender, receiver) = mpsc::channel();
    let worker_thread = thread::spawn(move || {
        info!("Worker thread started");
        let mut shutdown = false;
        let client = match ShardedClient::open(target) {
            Ok(client) => client,
            Err(err) => {
                running.store(false, Ordering::SeqCst);
                panic!(err)
            }
        };
        loop {
            match receiver.recv_timeout(Duration::from_millis(10)) {
                Ok(Message::Cmd(cmd)) => {
                    client.execute(cmd);
                }
                Ok(Message::Terminate) => {
                    shutdown = true;
                }
                Err(_) => {}
            }
            if shutdown {
                break;
            };
        }
        info!("Worker thread terminated");
    });
    EventHandlerImpl {
        worker: Worker { thread: Option::Some(worker_thread) },
        sender,
    }
}

pub(crate) fn new_cluster(target: Vec<String>, running: Arc<AtomicBool>) -> EventHandlerImpl {
    let (sender, receiver) = mpsc::channel();
    let worker_thread = thread::spawn(move || {
        info!("Worker thread started");
        let mut shutdown = false;
        let client = match ClusterClient::open(target) {
            Ok(client) => client,
            Err(err) => {
                running.store(false, Ordering::SeqCst);
                panic!(err);
            }
        };
        loop {
            match receiver.recv_timeout(Duration::from_millis(10)) {
                Ok(Message::Cmd(cmd)) => {
                    let mut conn = client.get_connection().expect("获取ClusterConnection失败");
                    cmd.execute(&mut conn);
                }
                Ok(Message::Terminate) => {
                    shutdown = true;
                }
                Err(_) => {}
            }
            if shutdown {
                break;
            };
        }
        info!("Worker thread terminated");
    });
    EventHandlerImpl {
        worker: Worker { thread: Option::Some(worker_thread) },
        sender,
    }
}

pub(crate) fn new(target: String, connect_timeout: Option<Duration>, running: Arc<AtomicBool>) -> EventHandlerImpl {
    let (sender, receiver) = mpsc::channel();
    let worker_thread = thread::spawn(move || {
        info!("Worker thread started");
        let mut conn = match connect(target.to_string(), connect_timeout) {
            Ok(conn) => conn,
            Err(err) => {
                running.store(false, Ordering::SeqCst);
                panic!("{}", err);
            }
        };
        let mut pipeline = redis::pipe();
        let mut count = 0;
        let mut timer = Instant::now();
        let hundred_millis = Duration::from_millis(100);
        let mut shutdown = false;
        loop {
            match receiver.recv_timeout(Duration::from_millis(10)) {
                Ok(Message::Cmd(cmd)) => {
                    pipeline.add_command(cmd);
                    count += 1;
                }
                Ok(Message::Terminate) => {
                    shutdown = true;
                }
                Err(_) => {}
            }
            let elapsed = timer.elapsed();
            if (elapsed.ge(&hundred_millis) || shutdown) && count > 0 {
                match pipeline.query(&mut conn) {
                    Err(err) => {
                        panic!("数据写入失败: {}", err);
                    }
                    Ok(()) => {
                        info!("写入成功: {}", count);
                    }
                };
                timer = Instant::now();
                pipeline.clear();
                count = 0;
            }
            if shutdown {
                break;
            };
        }
        info!("Worker thread terminated");
    });
    EventHandlerImpl {
        worker: Worker { thread: Option::Some(worker_thread) },
        sender,
    }
}

fn connect(addr: String, connect_timeout: Option<Duration>) -> io::Result<Connection> {
    let client = redis::Client::open(addr).unwrap();
    let connect_result = if connect_timeout.is_some() {
        client.get_connection_with_timeout(connect_timeout.unwrap())
    } else {
        client.get_connection()
    };
    match connect_result {
        Ok(connection) => {
            info!("连接到目的Redis成功");
            return Ok(connection);
        }
        Err(err) => {
            panic!("连接到目的Redis失败: {}", err);
        }
    }
}

struct Worker {
    thread: Option<thread::JoinHandle<()>>
}

enum Message {
    Cmd(redis::Cmd),
    Terminate,
}
