use std::cell::RefCell;
use std::collections::BTreeMap;
use std::sync::mpsc;
use std::sync::mpsc::Sender;

use murmurhash64::murmur_hash64a;
use redis::{Arg, Cmd, ConnectionAddr, IntoConnectionInfo};
use redis_event::cmd::Command;
use redis_event::Event::{AOF, RDB};
use redis_event::{Event, EventHandler};

use crate::command::CommandConverter;
use crate::worker::new_worker;
use crate::worker::{Message, Worker};

const SEED: u64 = 0x1234ABCD;

pub struct ShardedEventHandler {
    workers: Vec<Worker>,
    nodes: BTreeMap<u64, String>,
    senders: RefCell<BTreeMap<String, Sender<Message>>>,
}

impl EventHandler for ShardedEventHandler {
    fn handle(&mut self, event: Event) {
        match event {
            RDB(rdb) => self.handle_rdb(rdb),
            AOF(cmd) => match cmd {
                Command::SELECT(select) => {
                    let db = select.db.to_string();
                    self.broadcast("SELECT", Some(&vec![db.as_bytes()]));
                }
                Command::DEL(del) => {
                    for key in &del.keys {
                        let mut cmd = redis::cmd("DEL");
                        cmd.arg(key.as_slice());
                        self.execute(cmd, Some(key.as_slice()));
                    }
                }
                Command::MSET(mset) => {
                    for kv in &mset.key_values {
                        let mut cmd = redis::cmd("SET");
                        cmd.arg(kv.key).arg(kv.value);
                        self.execute(cmd, Some(kv.key));
                    }
                }
                Command::MSETNX(msetnx) => {
                    for kv in &msetnx.key_values {
                        let mut cmd = redis::cmd("SETNX");
                        cmd.arg(kv.key).arg(kv.value);
                        self.execute(cmd, Some(kv.key));
                    }
                }
                Command::PFCOUNT(pfcount) => {
                    for key in &pfcount.keys {
                        let mut cmd = redis::cmd("PFCOUNT");
                        cmd.arg(*key);
                        self.execute(cmd, Some(*key));
                    }
                }
                Command::UNLINK(unlink) => {
                    for key in &unlink.keys {
                        let mut cmd = redis::cmd("UNLINK");
                        cmd.arg(*key);
                        self.execute(cmd, Some(*key));
                    }
                }
                Command::SCRIPTFLUSH => {
                    self.broadcast("SCRIPT", Some(&vec!["FLUSH".as_bytes()]));
                }
                Command::SCRIPTLOAD(scriptload) => {
                    self.broadcast("SCRIPT", Some(&vec!["LOAD".as_bytes(), scriptload.script]));
                }
                Command::SWAPDB(swapdb) => {
                    self.broadcast("SWAPDB", Some(&vec![swapdb.index1, swapdb.index2]));
                }
                Command::FLUSHDB(flushdb) => {
                    match flushdb._async {
                        None => self.broadcast("FLUSHDB", None),
                        Some(_) => self.broadcast("FLUSHDB", Some(&vec!["ASYNC".as_bytes()])),
                    };
                }
                Command::FLUSHALL(flushall) => {
                    match flushall._async {
                        None => self.broadcast("FLUSHALL", None),
                        Some(_) => self.broadcast("FLUSHALL", Some(&vec!["ASYNC".as_bytes()])),
                    };
                }
                Command::XGROUP(xgroup) => {
                    if let Some(create) = &xgroup.create {
                        let mut cmd = redis::cmd("XGROUP");
                        cmd.arg("CREATE")
                            .arg(create.key)
                            .arg(create.group_name)
                            .arg(create.id);
                        self.execute(cmd, Some(create.key));
                    }
                    if let Some(set_id) = &xgroup.set_id {
                        let mut cmd = redis::cmd("XGROUP");
                        cmd.arg("SETID")
                            .arg(set_id.key)
                            .arg(set_id.group_name)
                            .arg(set_id.id);
                        self.execute(cmd, Some(set_id.key));
                    }
                    if let Some(destroy) = &xgroup.destroy {
                        let mut cmd = redis::cmd("XGROUP");
                        cmd.arg("DESTROY").arg(destroy.key).arg(destroy.group_name);
                        self.execute(cmd, Some(destroy.key));
                    }
                    if let Some(del_consumer) = &xgroup.del_consumer {
                        let mut cmd = redis::cmd("XGROUP");
                        cmd.arg("DELCONSUMER")
                            .arg(del_consumer.key)
                            .arg(del_consumer.group_name)
                            .arg(del_consumer.consumer_name);
                        self.execute(cmd, Some(del_consumer.key));
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
                _ => self.handle_aof(cmd),
            },
        };
    }
}

impl ShardedEventHandler {
    fn get_shard(&self, key: &[u8]) -> Option<String> {
        let hash = murmur_hash64a(key, SEED);
        if let Some((_, node)) = self.nodes.range(hash..).next() {
            Some(node.clone())
        } else {
            None
        }
    }

    fn broadcast(&self, cmd: &str, args: Option<&Vec<&[u8]>>) {
        let senders = self.senders.borrow();
        for (_, sender) in senders.iter() {
            let mut cmd = redis::cmd(cmd);
            if let Some(args) = args {
                for arg in args {
                    cmd.arg(*arg);
                }
            }
            if let Err(err) = sender.send(Message::Cmd(cmd)) {
                panic!("{}", err)
            }
        }
    }
}

impl Drop for ShardedEventHandler {
    fn drop(&mut self) {
        let senders = self.senders.borrow();
        for (_, sender) in senders.iter() {
            if let Err(_) = sender.send(Message::Terminate) {}
        }
        for worker in self.workers.iter_mut() {
            if let Some(thread) = worker.thread.take() {
                if let Err(_) = thread.join() {}
            }
        }
    }
}

impl CommandConverter for ShardedEventHandler {
    fn execute(&mut self, cmd: Cmd, key: Option<&[u8]>) {
        let _key;
        if let Some(the_key) = key {
            _key = the_key;
        } else {
            _key = match cmd.args_iter().skip(1).next() {
                None => panic!("cmd args is empty"),
                Some(arg) => match arg {
                    Arg::Simple(arg) => arg,
                    Arg::Cursor => panic!("cmd first arg is cursor"),
                },
            };
        }
        let senders = self.senders.borrow();
        match self.get_shard(&_key) {
            None => {
                let first_sender = senders.values().next();
                let sender = first_sender.unwrap();
                if let Err(err) = sender.send(Message::Cmd(cmd)) {
                    panic!("{}", err)
                }
            }
            Some(node) => {
                let sender = senders.get(&node).unwrap();
                if let Err(err) = sender.send(Message::Cmd(cmd)) {
                    panic!("{}", err)
                }
            }
        }
    }
}

pub(crate) fn new_sharded(
    initial_nodes: Vec<String>,
    batch_size: i32,
    flush_interval: u64,
) -> ShardedEventHandler {
    let mut senders: BTreeMap<String, Sender<Message>> = BTreeMap::new();
    let mut workers = Vec::new();
    let mut nodes: BTreeMap<u64, String> = BTreeMap::new();

    for (i, node) in initial_nodes.into_iter().enumerate() {
        let info = node.as_str().into_connection_info().unwrap();
        let addr = match *info.addr {
            ConnectionAddr::Tcp(ref host, port) => format!("{}-{}:{}", i, host, port),
            _ => panic!("No reach."),
        };
        for n in 0..160 {
            let name = format!("SHARD-{}-NODE-{}", i, n);
            let hash = murmur_hash64a(name.as_bytes(), SEED);
            nodes.insert(hash, addr.clone());
        }
        let (sender, receiver) = mpsc::channel();
        let worker_name = format!("shard-{}", addr);
        let worker = new_worker(
            node.clone(),
            receiver,
            &worker_name,
            batch_size,
            flush_interval,
        );
        senders.insert(addr, sender);
        workers.push(Worker {
            thread: Some(worker),
        });
    }
    ShardedEventHandler {
        workers,
        nodes,
        senders: RefCell::new(senders),
    }
}
