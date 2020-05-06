use std::sync::mpsc;
use std::sync::mpsc::Sender;

use redis_event::{Event, EventHandler};
use redis_event::Event::{AOF, RDB};
use redis_event::rdb::Object;

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
                match rdb {
                    Object::Stream(key, stream) => {
                        for (id, entry) in stream.entries {
                            let mut cmd = redis::cmd("XADD");
                            cmd.arg(key.as_slice());
                            let id = format!("{}-{}", id.ms, id.seq);
                            cmd.arg(id);
                            for (field, value) in entry.fields {
                                cmd.arg(field).arg(value);
                            }
                            if let Err(err) = self.sender.send(Message::Cmd(cmd)) {
                                panic!("{}", err)
                            }
                        }
                        None
                    }
                    _ => self.handle_rdb(rdb)
                }
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

pub(crate) fn new(target: String, batch_size: i32, flush_interval: u64) -> EventHandlerImpl {
    let (sender, receiver) = mpsc::channel();
    let worker_thread = worker::new_worker(target, receiver, "copy_redis::worker", batch_size, flush_interval);
    EventHandlerImpl {
        worker: Worker { thread: Option::Some(worker_thread) },
        sender,
    }
}
