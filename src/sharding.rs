use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap};

use murmurhash64::murmur_hash64a;
use redis::{Arg, Cmd, Connection, ConnectionAddr, IntoConnectionInfo, RedisResult};

const SEED: u64 = 0x1234ABCD;

pub struct ShardedClient {
    nodes: BTreeMap<u64, String>,
    resources: RefCell<HashMap<String, Connection>>,
}

impl ShardedClient {
    pub fn open<T: IntoConnectionInfo>(initial_nodes: Vec<T>) -> RedisResult<ShardedClient> {
        let mut connections: HashMap<String, Connection> = HashMap::with_capacity(initial_nodes.len());
        let mut nodes: BTreeMap<u64, String> = BTreeMap::new();
        for (i, node) in initial_nodes.into_iter().enumerate() {
            let info = node.into_connection_info()?;
            let addr = match *info.addr {
                ConnectionAddr::Tcp(ref host, port) => format!("{}:{}", host, port),
                _ => panic!("No reach."),
            };
            let client = redis::Client::open(info)?;
            let conn = client.get_connection()?;
            
            for n in 0..160 {
                let name = format!("SHARD-{}-NODE-{}", i, n);
                let hash = murmur_hash64a(name.as_bytes(), SEED);
                nodes.insert(hash, addr.clone());
            }
            connections.insert(addr, conn);
        }
        Ok(ShardedClient { nodes, resources: RefCell::new(connections) })
    }
    
    pub fn execute(&self, cmd: Cmd) {
        let key = match cmd.args_iter().skip(1).next() {
            None => { panic!("cmd args is empty") }
            Some(arg) => {
                match arg {
                    Arg::Simple(arg) => { arg }
                    Arg::Cursor => { panic!("cmd first arg is cursor") }
                }
            }
        };
        println!("{}", String::from_utf8_lossy(key));
        
        let hash = murmur_hash64a(key, SEED);
        let conn;
        let mut entry = self.resources.borrow_mut();
        let mut values = entry.values_mut().next();
        let option;
        if let Some((_, node)) = self.nodes.range(hash..).next() {
            option = entry.get_mut(node);
            conn = option.unwrap();
        } else {
            conn = values.as_mut().unwrap();
        }
        cmd.execute(conn);
    }
}

#[cfg(test)]
mod tests {
    use crate::sharding::ShardedClient;
    
    // #[test]
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