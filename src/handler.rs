use std::sync::mpsc::Sender;
use std::sync::{mpsc, Arc};

use redis_event::rdb::Object;
use redis_event::Event::{AOF, RDB};
use redis_event::{Event, EventHandler};

use crate::command::CommandConverter;
use crate::worker;
use crate::worker::{Message, Worker};
use redis::Cmd;
use std::sync::atomic::AtomicBool;

pub(crate) struct EventHandlerImpl {
    worker: Worker,
    sender: Sender<Message>,
}

impl EventHandler for EventHandlerImpl {
    fn handle(&mut self, event: Event) {
        match event {
            RDB(rdb) => match rdb {
                Object::Stream(key, stream) => {
                    for (id, entry) in stream.entries {
                        let mut cmd = redis::cmd("XADD");
                        cmd.arg(key.as_slice());
                        cmd.arg(id.to_string());
                        for (field, value) in entry.fields {
                            cmd.arg(field).arg(value);
                        }
                        self.execute(cmd, None);
                    }
                    for group in stream.groups {
                        let mut cmd = redis::cmd("XGROUP");
                        cmd.arg("CREATE")
                            .arg(key.as_slice())
                            .arg(group.name)
                            .arg(group.last_id.to_string());

                        self.execute(cmd, None);
                    }
                }
                _ => self.handle_rdb(rdb),
            },
            AOF(cmd) => {
                self.handle_aof(cmd);
            }
        };
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

impl CommandConverter for EventHandlerImpl {
    fn execute(&mut self, cmd: Cmd, _: Option<&[u8]>) {
        if let Err(err) = self.sender.send(Message::Cmd(cmd)) {
            panic!("{}", err)
        }
    }
}

pub(crate) fn new(
    target: String,
    batch_size: i32,
    flush_interval: u64,
    control_flag: Arc<AtomicBool>,
) -> EventHandlerImpl {
    let (sender, receiver) = mpsc::channel();
    let worker_thread = worker::new_worker(
        target,
        receiver,
        "copy_redis::worker",
        batch_size,
        flush_interval,
        control_flag,
    );
    EventHandlerImpl {
        worker: Worker {
            thread: Option::Some(worker_thread),
        },
        sender,
    }
}
