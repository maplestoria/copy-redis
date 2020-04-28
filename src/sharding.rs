use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap};
use std::sync::mpsc;
use std::sync::mpsc::Sender;

use log::info;
use murmurhash64::murmur_hash64a;
use redis::{Arg, Cmd, ConnectionAddr, IntoConnectionInfo};
use redis_event::{Event, EventHandler};
use redis_event::cmd::Command;
use redis_event::Event::{AOF, RDB};

use crate::command::CommandConverter;
use crate::worker::{Message, Worker};
use crate::worker::new_worker;

const SEED: u64 = 0x1234ABCD;

pub struct ShardedEventHandler {
    workers: Vec<Worker>,
    nodes: BTreeMap<u64, String>,
    senders: RefCell<HashMap<String, Sender<Message>>>,
}

impl EventHandler for ShardedEventHandler {
    fn handle(&mut self, event: Event) {
        let cmd = match event {
            RDB(rdb) => {
                self.handle_rdb(rdb)
            }
            AOF(cmd) => {
                match cmd {
                    Command::SELECT(select) => {
                        info!("所有shard切换至db: {}", select.db);
                        self.select_db(select.db);
                        None
                    }
                    _ => self.handle_aof(cmd)
                }
            }
        };
        self.execute(cmd);
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
    
    fn select_db(&self, db: i32) {
        let senders = self.senders.borrow();
        for (_, sender) in senders.iter() {
            let mut cmd = redis::cmd("SELECT");
            cmd.arg(db);
            if let Err(err) = sender.send(Message::Cmd(cmd)) {
                panic!("{}", err)
            }
        }
    }
    
    fn execute(&self, cmd: Option<Cmd>) {
        if let Some(cmd) = cmd {
            let key = match cmd.args_iter().skip(1).next() {
                None => panic!("cmd args is empty"),
                Some(arg) => {
                    match arg {
                        Arg::Simple(arg) => arg,
                        Arg::Cursor => panic!("cmd first arg is cursor")
                    }
                }
            };
            let senders = self.senders.borrow();
            let first_sender = senders.values().next();
            match self.get_shard(key) {
                None => {
                    let sender = first_sender.unwrap();
                    if let Err(err) = sender.send(Message::Cmd(cmd)) {
                        panic!("{}", err)
                    }
                }
                Some(node) => {
                    let sender = senders.get(&node);
                    let sender = sender.unwrap();
                    if let Err(err) = sender.send(Message::Cmd(cmd)) {
                        panic!("{}", err)
                    }
                }
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

pub(crate) fn new_sharded(initial_nodes: Vec<String>) -> ShardedEventHandler {
    let mut senders: HashMap<String, Sender<Message>> = HashMap::with_capacity(initial_nodes.len());
    let mut workers = Vec::new();
    let mut nodes: BTreeMap<u64, String> = BTreeMap::new();
    
    for (i, node) in initial_nodes.into_iter().enumerate() {
        let info = node.as_str().into_connection_info().unwrap();
        let addr = match *info.addr {
            ConnectionAddr::Tcp(ref host, port) => format!("{}:{}", host, port),
            _ => panic!("No reach."),
        };
        for n in 0..160 {
            let name = format!("SHARD-{}-NODE-{}", i, n);
            let hash = murmur_hash64a(name.as_bytes(), SEED);
            nodes.insert(hash, addr.clone());
        }
        let (sender, receiver) = mpsc::channel();
        let worker_name = format!("shard-{}-{}", i, addr);
        let worker = new_worker(node.clone(), receiver, &worker_name);
        senders.insert(addr, sender);
        workers.push(Worker { thread: Some(worker) });
    }
    ShardedEventHandler {
        workers,
        nodes,
        senders: RefCell::new(senders),
    }
}

#[cfg(test)]
mod tests {
    use crate::sharding::ShardedClient;
    
    #[test]
    fn test_client() {
        let nodes = vec!["redis://127.0.0.1:6379/", "redis://127.0.0.1:6479/"];
        let client = ShardedClient::open(nodes).unwrap();
        let mut cmd = redis::cmd("set");
        cmd.arg("helloworld").arg("回复开始打卡的");
        client.execute(cmd);
        cmd = redis::cmd("set");
        cmd.arg("aaa").arg("aaa");
        client.execute(cmd);
    }
}