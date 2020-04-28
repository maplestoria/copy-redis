use std::sync::{Arc, mpsc};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::Sender;
use std::thread;
use std::time::Duration;

use log::info;
use redis::cluster::ClusterClient;
use redis_event::{Event, EventHandler};
use redis_event::Event::{AOF, RDB};

use crate::command::CommandConverter;
use crate::worker;
use crate::worker::{Message, Worker};

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

pub(crate) fn new(target: String) -> EventHandlerImpl {
    let (sender, receiver) = mpsc::channel();
    let worker_thread = worker::new_worker(target, receiver, "copy_redis::worker");
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
