use std::sync::mpsc;
use std::sync::mpsc::Sender;
use std::thread;
use std::time::{Duration, Instant};

use log::{error, info};
use redis::Cmd;
use redis_event::{Event, EventHandler};
use redis_event::cmd::Command;
use redis_event::cmd::keys::ORDER;
use redis_event::cmd::lists::POSITION;
use redis_event::cmd::sorted_sets::AGGREGATE;
use redis_event::cmd::strings::{ExistType, ExpireType, Op, Operation, Overflow};
use redis_event::Event::{AOF, RDB};
use redis_event::rdb::Object;

pub(crate) struct EventHandlerImpl {
    worker: Worker,
    sender: Sender<Message>,
    db: isize,
}

impl EventHandler for EventHandlerImpl {
    fn handle(&mut self, event: Event) {
        match event {
            RDB(rdb) => {
                self.handle_rdb(rdb);
            }
            AOF(cmd) => {
                self.handle_aof(cmd);
            }
        }
    }
}

impl EventHandlerImpl {
    fn send(&mut self, cmd: Cmd) {
        self.sender.send(Message::Cmd(cmd)).expect("发送消息失败");
    }
    
    fn handle_rdb(&mut self, rdb: Object) {
        match rdb {
            Object::String(kv) => {
                if kv.meta.db != self.db {
                    let mut cmd = redis::cmd("select");
                    cmd.arg(kv.meta.db);
                    self.send(cmd);
                    self.db = kv.meta.db;
                }
                let mut cmd = redis::cmd("set");
                cmd.arg(kv.key).arg(kv.value);
                self.send(cmd);
            }
            Object::List(list) => {
                if list.meta.db != self.db {
                    let mut cmd = redis::cmd("select");
                    cmd.arg(list.meta.db);
                    self.send(cmd);
                    self.db = list.meta.db;
                }
                let mut cmd = redis::cmd("rpush");
                cmd.arg(list.key);
                for val in list.values {
                    cmd.arg(val.as_slice());
                }
                self.send(cmd);
            }
            Object::Set(set) => {
                if set.meta.db != self.db {
                    let mut cmd = redis::cmd("select");
                    cmd.arg(set.meta.db);
                    self.send(cmd);
                    self.db = set.meta.db;
                }
                let mut cmd = redis::cmd("sadd");
                cmd.arg(set.key);
                for member in set.members {
                    cmd.arg(member.as_slice());
                }
                self.send(cmd);
            }
            Object::SortedSet(sorted_set) => {
                if sorted_set.meta.db != self.db {
                    let mut cmd = redis::cmd("select");
                    cmd.arg(sorted_set.meta.db);
                    self.send(cmd);
                    self.db = sorted_set.meta.db;
                }
                let mut cmd = redis::cmd("zadd");
                cmd.arg(sorted_set.key);
                for item in sorted_set.items {
                    cmd.arg(item.score).arg(item.member.as_slice());
                }
                self.send(cmd);
            }
            Object::Hash(hash) => {
                if hash.meta.db != self.db {
                    let mut cmd = redis::cmd("select");
                    cmd.arg(hash.meta.db);
                    self.send(cmd);
                    self.db = hash.meta.db;
                }
                let mut cmd = redis::cmd("hmset");
                cmd.arg(hash.key);
                for field in hash.fields {
                    cmd.arg(field.name.as_slice()).arg(field.value.as_slice());
                }
                self.send(cmd);
            }
            _ => {}
        }
    }
    
    fn handle_aof(&mut self, cmd: Command) {
        info!("{:?}", cmd);
        match cmd {
            Command::APPEND(append) => {
                let mut cmd = redis::cmd("APPEND");
                cmd.arg(append.key).arg(append.value);
                self.send(cmd);
            }
            Command::BITFIELD(bitfield) => {
                let mut cmd = redis::cmd("BITFIELD");
                cmd.arg(bitfield.key);
                if let Some(statement) = &bitfield.statements {
                    for op in statement {
                        match op {
                            Operation::GET(get) => {
                                cmd.arg("GET").arg(get._type).arg(get.offset);
                            }
                            Operation::INCRBY(incrby) => {
                                cmd.arg("INCRBY").arg(incrby._type).arg(incrby.offset).arg(incrby.increment);
                            }
                            Operation::SET(set) => {
                                cmd.arg("SET").arg(set._type).arg(set.offset).arg(set.value);
                            }
                        }
                    }
                }
                if let Some(overflow) = &bitfield.overflows {
                    for of in overflow {
                        match of {
                            Overflow::WRAP => {
                                cmd.arg("OVERFLOW").arg("WRAP");
                            }
                            Overflow::SAT => {
                                cmd.arg("OVERFLOW").arg("SAT");
                            }
                            Overflow::FAIL => {
                                cmd.arg("OVERFLOW").arg("FAIL");
                            }
                        }
                    }
                }
                self.send(cmd);
            }
            Command::BITOP(bitop) => {
                let mut cmd = redis::cmd("BITOP");
                match bitop.operation {
                    Op::AND => {
                        cmd.arg("AND");
                    }
                    Op::OR => {
                        cmd.arg("OR");
                    }
                    Op::XOR => {
                        cmd.arg("XOR");
                    }
                    Op::NOT => {
                        cmd.arg("NOT");
                    }
                }
                cmd.arg(bitop.dest_key);
                for key in &bitop.keys {
                    cmd.arg(key.as_slice());
                }
                self.send(cmd);
            }
            Command::BRPOPLPUSH(brpoplpush) => {
                let mut cmd = redis::cmd("BRPOPLPUSH");
                cmd.arg(brpoplpush.source)
                    .arg(brpoplpush.destination)
                    .arg(brpoplpush.timeout);
                self.send(cmd);
            }
            Command::DECR(decr) => {
                let mut cmd = redis::cmd("DECR");
                cmd.arg(decr.key);
                self.send(cmd);
            }
            Command::DECRBY(decrby) => {
                let mut cmd = redis::cmd("DECRBY");
                cmd.arg(decrby.key).arg(decrby.decrement);
                self.send(cmd);
            }
            Command::DEL(del) => {
                let mut cmd = redis::cmd("DEL");
                for key in &del.keys {
                    cmd.arg(key.as_slice());
                }
                self.send(cmd);
            }
            Command::EVAL(eval) => {
                let mut cmd = redis::cmd("EVAL");
                cmd.arg(eval.script).arg(eval.num_keys);
                for key in &eval.keys {
                    cmd.arg(*key);
                }
                for arg in &eval.args {
                    cmd.arg(*arg);
                }
                self.send(cmd);
            }
            Command::EVALSHA(evalsha) => {
                let mut cmd = redis::cmd("EVALSHA");
                cmd.arg(evalsha.sha1).arg(evalsha.num_keys);
                for key in &evalsha.keys {
                    cmd.arg(*key);
                }
                for arg in &evalsha.args {
                    cmd.arg(*arg);
                }
                self.send(cmd);
            }
            Command::EXPIRE(expire) => {
                let mut cmd = redis::cmd("EXPIRE");
                cmd.arg(expire.key).arg(expire.seconds);
                self.send(cmd);
            }
            Command::EXPIREAT(expireat) => {
                let mut cmd = redis::cmd("EXPIREAT");
                cmd.arg(expireat.key).arg(expireat.timestamp);
                self.send(cmd);
            }
            Command::EXEC => {
                let cmd = redis::cmd("EXEC");
                self.send(cmd);
            }
            Command::FLUSHALL(flushall) => {
                let mut cmd = redis::cmd("FLUSHALL");
                if flushall._async.is_some() {
                    cmd.arg("ASYNC");
                }
                self.send(cmd);
            }
            Command::FLUSHDB(flushdb) => {
                let mut cmd = redis::cmd("FLUSHDB");
                if flushdb._async.is_some() {
                    cmd.arg("ASYNC");
                }
                self.send(cmd);
            }
            Command::GETSET(getset) => {
                let mut cmd = redis::cmd("GETSET");
                cmd.arg(getset.key).arg(getset.value);
                self.send(cmd);
            }
            Command::HDEL(hdel) => {
                let mut cmd = redis::cmd("HDEL");
                cmd.arg(hdel.key);
                for field in &hdel.fields {
                    cmd.arg(*field);
                }
                self.send(cmd);
            }
            Command::HINCRBY(hincrby) => {
                let mut cmd = redis::cmd("HINCRBY");
                cmd.arg(hincrby.key).arg(hincrby.field).arg(hincrby.increment);
                self.send(cmd);
            }
            Command::HMSET(hmset) => {
                let mut cmd = redis::cmd("HMSET");
                cmd.arg(hmset.key);
                for field in &hmset.fields {
                    cmd.arg(field.name).arg(field.value);
                }
                self.send(cmd);
            }
            Command::HSET(hset) => {
                let mut cmd = redis::cmd("HSET");
                cmd.arg(hset.key);
                for field in &hset.fields {
                    cmd.arg(field.name).arg(field.value);
                }
                self.send(cmd);
            }
            Command::HSETNX(hsetnx) => {
                let mut cmd = redis::cmd("HSETNX");
                cmd.arg(hsetnx.key).arg(hsetnx.field).arg(hsetnx.value);
                self.send(cmd);
            }
            Command::INCR(incr) => {
                let mut cmd = redis::cmd("INCR");
                cmd.arg(incr.key);
                self.send(cmd);
            }
            Command::INCRBY(incrby) => {
                let mut cmd = redis::cmd("INCRBY");
                cmd.arg(incrby.key).arg(incrby.increment);
                self.send(cmd);
            }
            Command::LINSERT(linsert) => {
                let mut cmd = redis::cmd("LINSERT");
                cmd.arg(linsert.key);
                match linsert.position {
                    POSITION::BEFORE => {
                        cmd.arg("BEFORE");
                    }
                    POSITION::AFTER => {
                        cmd.arg("AFTER");
                    }
                }
                cmd.arg(linsert.pivot).arg(linsert.element);
                self.send(cmd);
            }
            Command::LPOP(lpop) => {
                let mut cmd = redis::cmd("LPOP");
                cmd.arg(lpop.key);
                self.send(cmd);
            }
            Command::LPUSH(lpush) => {
                let mut cmd = redis::cmd("LPUSH");
                cmd.arg(lpush.key);
                for element in &lpush.elements {
                    cmd.arg(*element);
                }
                self.send(cmd);
            }
            Command::LPUSHX(lpushx) => {
                let mut cmd = redis::cmd("LPUSHX");
                cmd.arg(lpushx.key);
                for element in &lpushx.elements {
                    cmd.arg(*element);
                }
                self.send(cmd);
            }
            Command::LREM(lrem) => {
                let mut cmd = redis::cmd("LREM");
                cmd.arg(lrem.key).arg(lrem.count).arg(lrem.element);
                self.send(cmd);
            }
            Command::LSET(lset) => {
                let mut cmd = redis::cmd("LSET");
                cmd.arg(lset.key).arg(lset.index).arg(lset.element);
                self.send(cmd);
            }
            Command::LTRIM(ltrim) => {
                let mut cmd = redis::cmd("LTRIM");
                cmd.arg(ltrim.key).arg(ltrim.start).arg(ltrim.stop);
                self.send(cmd);
            }
            Command::MOVE(_move) => {
                let mut cmd = redis::cmd("MOVE");
                cmd.arg(_move.key).arg(_move.db);
                self.send(cmd);
            }
            Command::MSET(mset) => {
                let mut cmd = redis::cmd("MSET");
                for kv in &mset.key_values {
                    cmd.arg(kv.key).arg(kv.value);
                }
                self.send(cmd);
            }
            Command::MSETNX(msetnx) => {
                let mut cmd = redis::cmd("MSETNX");
                for kv in &msetnx.key_values {
                    cmd.arg(kv.key).arg(kv.value);
                }
                self.send(cmd);
            }
            Command::MULTI => {
                let cmd = redis::cmd("MULTI");
                self.send(cmd);
            }
            Command::PERSIST(persist) => {
                let mut cmd = redis::cmd("PERSIST");
                cmd.arg(persist.key);
                self.send(cmd);
            }
            Command::PEXPIRE(pexpire) => {
                let mut cmd = redis::cmd("PEXPIRE");
                cmd.arg(pexpire.milliseconds);
                self.send(cmd);
            }
            Command::PEXPIREAT(pexpireat) => {
                let mut cmd = redis::cmd("PEXPIREAT");
                cmd.arg(pexpireat.mill_timestamp);
                self.send(cmd);
            }
            Command::PFADD(pfadd) => {
                let mut cmd = redis::cmd("PFADD");
                cmd.arg(pfadd.key);
                for element in &pfadd.elements {
                    cmd.arg(*element);
                }
                self.send(cmd);
            }
            Command::PFCOUNT(pfcount) => {
                let mut cmd = redis::cmd("PFCOUNT");
                for key in &pfcount.keys {
                    cmd.arg(*key);
                }
                self.send(cmd);
            }
            Command::PFMERGE(pfmerge) => {
                let mut cmd = redis::cmd("PFMERGE");
                cmd.arg(pfmerge.dest_key);
                for key in &pfmerge.source_keys {
                    cmd.arg(*key);
                }
                self.send(cmd);
            }
            Command::PSETEX(psetex) => {
                let mut cmd = redis::cmd("PSETEX");
                cmd.arg(psetex.key).arg(psetex.milliseconds).arg(psetex.value);
                self.send(cmd);
            }
            Command::PUBLISH(publish) => {
                let mut cmd = redis::cmd("PUBLISH");
                cmd.arg(publish.channel).arg(publish.message);
                self.send(cmd);
            }
            Command::RENAME(rename) => {
                let mut cmd = redis::cmd("RENAME");
                cmd.arg(rename.key).arg(rename.new_key);
                self.send(cmd);
            }
            Command::RENAMENX(renamenx) => {
                let mut cmd = redis::cmd("RENAMENX");
                cmd.arg(renamenx.key).arg(renamenx.new_key);
                self.send(cmd);
            }
            Command::RESTORE(restore) => {
                let mut cmd = redis::cmd("RESTORE");
                cmd.arg(restore.key).arg(restore.ttl).arg(restore.value);
                if restore.replace.is_some() {
                    cmd.arg("REPLACE");
                }
                if restore.abs_ttl.is_some() {
                    cmd.arg("ABSTTL");
                }
                if let Some(idle) = restore.idle_time {
                    cmd.arg("IDLETIME").arg(idle);
                }
                if let Some(freq) = restore.freq {
                    cmd.arg("FREQ").arg(freq);
                }
                self.send(cmd);
            }
            Command::RPOP(rpop) => {
                let mut cmd = redis::cmd("RPOP");
                cmd.arg(rpop.key);
                self.send(cmd);
            }
            Command::RPOPLPUSH(rpoplpush) => {
                let mut cmd = redis::cmd("RPOPLPUSH");
                cmd.arg(rpoplpush.source).arg(rpoplpush.destination);
                self.send(cmd);
            }
            Command::RPUSH(rpush) => {
                let mut cmd = redis::cmd("RPUSH");
                cmd.arg(rpush.key);
                for element in &rpush.elements {
                    cmd.arg(*element);
                }
                self.send(cmd);
            }
            Command::RPUSHX(rpushx) => {
                let mut cmd = redis::cmd("RPUSHX");
                cmd.arg(rpushx.key);
                for element in &rpushx.elements {
                    cmd.arg(*element);
                }
                self.send(cmd);
            }
            Command::SADD(sadd) => {
                let mut cmd = redis::cmd("SADD");
                cmd.arg(sadd.key);
                for member in &sadd.members {
                    cmd.arg(*member);
                }
                self.send(cmd);
            }
            Command::SCRIPTFLUSH => {
                let cmd = redis::cmd("SCRIPT FLUSH");
                self.send(cmd);
            }
            Command::SCRIPTLOAD(scriptload) => {
                let mut cmd = redis::cmd("SCRIPT LOAD");
                cmd.arg(scriptload.script);
                self.send(cmd);
            }
            Command::SDIFFSTORE(sdiffstore) => {
                let mut cmd = redis::cmd("SDIFFSTORE");
                cmd.arg(sdiffstore.destination);
                for key in &sdiffstore.keys {
                    cmd.arg(*key);
                }
                self.send(cmd);
            }
            Command::SET(set) => {
                let mut cmd = redis::cmd("SET");
                cmd.arg(set.key).arg(set.value);
                if let Some((expire_type, value)) = set.expire.as_ref() {
                    match expire_type {
                        ExpireType::EX => {
                            cmd.arg("EX").arg(value.as_slice());
                        }
                        ExpireType::PX => {
                            cmd.arg("PX").arg(value.as_slice());
                        }
                    }
                }
                if let Some(exist) = set.exist_type.as_ref() {
                    match exist {
                        ExistType::NX => {
                            cmd.arg("NX");
                        }
                        ExistType::XX => {
                            cmd.arg("XX");
                        }
                    }
                }
                self.send(cmd);
            }
            Command::SETBIT(setbit) => {
                let mut cmd = redis::cmd("SETBIT");
                cmd.arg(setbit.key).arg(setbit.offset).arg(setbit.value);
                self.send(cmd);
            }
            Command::SETEX(setex) => {
                let mut cmd = redis::cmd("SETEX");
                cmd.arg(setex.key).arg(setex.seconds).arg(setex.value);
                self.send(cmd);
            }
            Command::SETNX(setnx) => {
                let mut cmd = redis::cmd("SETNX");
                cmd.arg(setnx.key).arg(setnx.value);
                self.send(cmd);
            }
            Command::SELECT(select) => {
                let mut cmd = redis::cmd("SELECT");
                cmd.arg(select.db);
                self.send(cmd);
            }
            Command::SETRANGE(setrange) => {
                let mut cmd = redis::cmd("SETRANGE");
                cmd.arg(setrange.key).arg(setrange.offset).arg(setrange.value);
                self.send(cmd);
            }
            Command::SINTERSTORE(sinterstore) => {
                let mut cmd = redis::cmd("SINTERSTORE");
                cmd.arg(sinterstore.destination);
                for key in &sinterstore.keys {
                    cmd.arg(*key);
                }
                self.send(cmd);
            }
            Command::SMOVE(smove) => {
                let mut cmd = redis::cmd("SMOVE");
                cmd.arg(smove.source).arg(smove.destination).arg(smove.member);
                self.send(cmd);
            }
            Command::SORT(sort) => {
                let mut cmd = redis::cmd("SORT");
                cmd.arg(sort.key);
                if let Some(pattern) = sort.by_pattern {
                    cmd.arg("BY").arg(pattern);
                }
                if let Some(limit) = &sort.limit {
                    cmd.arg("LIMIT").arg(limit.offset).arg(limit.count);
                }
                if let Some(get_patterns) = &sort.get_patterns {
                    for pattern in get_patterns {
                        cmd.arg("GET").arg(*pattern);
                    }
                }
                if let Some(order) = &sort.order {
                    match order {
                        ORDER::ASC => {
                            cmd.arg("ASC");
                        }
                        ORDER::DESC => {
                            cmd.arg("DESC");
                        }
                    }
                }
                if sort.alpha.is_some() {
                    cmd.arg("ALPHA");
                }
                if let Some(dest) = sort.destination {
                    cmd.arg("STORE").arg(dest);
                }
                self.send(cmd);
            }
            Command::SREM(srem) => {
                let mut cmd = redis::cmd("SREM");
                cmd.arg(srem.key);
                for member in &srem.members {
                    cmd.arg(*member);
                }
                self.send(cmd);
            }
            Command::SUNIONSTORE(sunion) => {
                let mut cmd = redis::cmd("SUNIONSTORE");
                cmd.arg(sunion.destination);
                for key in &sunion.keys {
                    cmd.arg(*key);
                }
                self.send(cmd);
            }
            Command::SWAPDB(swapdb) => {
                let mut cmd = redis::cmd("SWAPDB");
                cmd.arg(swapdb.index1).arg(swapdb.index2);
                self.send(cmd);
            }
            Command::UNLINK(unlink) => {
                let mut cmd = redis::cmd("UNLINK");
                for key in &unlink.keys {
                    cmd.arg(*key);
                }
                self.send(cmd);
            }
            Command::ZADD(zadd) => {
                let mut cmd = redis::cmd("ZADD");
                cmd.arg(zadd.key);
                if let Some(exist_type) = &zadd.exist_type {
                    match exist_type {
                        ExistType::NX => {
                            cmd.arg("NX");
                        }
                        ExistType::XX => {
                            cmd.arg("XX");
                        }
                    }
                }
                if let Some(_) = &zadd.ch {
                    cmd.arg("CH");
                }
                if let Some(_) = &zadd.incr {
                    cmd.arg("INCR");
                }
                for item in &zadd.items {
                    cmd.arg(item.score).arg(item.member);
                }
                self.send(cmd);
            }
            Command::ZINCRBY(zincrby) => {
                let mut cmd = redis::cmd("ZINCRBY");
                cmd.arg(zincrby.key).arg(zincrby.increment).arg(zincrby.member);
                self.send(cmd);
            }
            Command::ZINTERSTORE(zinterstore) => {
                let mut cmd = redis::cmd("ZINTERSTORE");
                cmd.arg(zinterstore.destination).arg(zinterstore.num_keys);
                for key in &zinterstore.keys {
                    cmd.arg(*key);
                }
                if let Some(weights) = &zinterstore.weights {
                    cmd.arg("WEIGHTS");
                    for weight in weights {
                        cmd.arg(*weight);
                    }
                }
                if let Some(aggregate) = &zinterstore.aggregate {
                    cmd.arg("AGGREGATE");
                    match aggregate {
                        AGGREGATE::SUM => { cmd.arg("SUM"); }
                        AGGREGATE::MIN => { cmd.arg("MIN"); }
                        AGGREGATE::MAX => { cmd.arg("MAX"); }
                    }
                }
                self.send(cmd);
            }
            Command::ZPOPMAX(zpopmax) => {
                let mut cmd = redis::cmd("ZPOPMAX");
                cmd.arg(zpopmax.key);
                if let Some(count) = zpopmax.count {
                    cmd.arg(count);
                }
                self.send(cmd);
            }
            Command::ZPOPMIN(zpopmin) => {
                let mut cmd = redis::cmd("ZPOPMIN");
                cmd.arg(zpopmin.key);
                if let Some(count) = zpopmin.count {
                    cmd.arg(count);
                }
                self.send(cmd);
            }
            Command::ZREM(zrem) => {
                let mut cmd = redis::cmd("ZREM");
                cmd.arg(zrem.key);
                for member in &zrem.members {
                    cmd.arg(*member);
                }
                self.send(cmd);
            }
            Command::ZREMRANGEBYLEX(zrem) => {
                let mut cmd = redis::cmd("ZREMRANGEBYLEX");
                cmd.arg(zrem.key).arg(zrem.min).arg(zrem.max);
                self.send(cmd);
            }
            Command::ZREMRANGEBYRANK(zrem) => {
                let mut cmd = redis::cmd("ZREMRANGEBYRANK");
                cmd.arg(zrem.key).arg(zrem.start).arg(zrem.stop);
                self.send(cmd);
            }
            Command::ZREMRANGEBYSCORE(zrem) => {
                let mut cmd = redis::cmd("ZREMRANGEBYSCORE");
                cmd.arg(zrem.key).arg(zrem.min).arg(zrem.max);
                self.send(cmd);
            }
            Command::ZUNIONSTORE(zunion) => {
                let mut cmd = redis::cmd("ZUNIONSTORE");
                cmd.arg(zunion.destination).arg(zunion.destination).arg(zunion.num_keys);
                for key in &zunion.keys {
                    cmd.arg(*key);
                }
                if let Some(weights) = &zunion.weights {
                    cmd.arg("WEIGHTS");
                    for weight in weights {
                        cmd.arg(*weight);
                    }
                }
                if let Some(aggregate) = &zunion.aggregate {
                    cmd.arg("AGGREGATE");
                    match aggregate {
                        AGGREGATE::SUM => {
                            cmd.arg("SUM");
                        }
                        AGGREGATE::MIN => {
                            cmd.arg("MIN");
                        }
                        AGGREGATE::MAX => {
                            cmd.arg("MAX");
                        }
                    }
                }
                self.send(cmd);
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

pub(crate) fn new(target: &str, connect_timeout: Duration) -> EventHandlerImpl {
    let addr = target.to_string();
    let (sender, receiver) = mpsc::channel();
    let worker_thread = thread::spawn(move || {
        info!("Worker thread started");
        let client = redis::Client::open(addr).unwrap();
        let mut conn = client.get_connection_with_timeout(connect_timeout).expect("连接到目的Redis失败");
        let mut pipeline = redis::pipe();
        let mut count = 0;
        let mut timer = Instant::now();
        let hundred_millis = Duration::from_millis(100);
        let mut shutdown = false;
        loop {
            match receiver.recv_timeout(Duration::from_millis(1)) {
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
            if (elapsed.ge(&hundred_millis) && count > 0) || shutdown {
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
            if shutdown {
                break;
            };
        }
        info!("Worker thread terminated");
    });
    EventHandlerImpl {
        worker: Worker { thread: Option::Some(worker_thread) },
        sender,
        db: -1,
    }
}

struct Worker {
    thread: Option<thread::JoinHandle<()>>
}

enum Message {
    Cmd(redis::Cmd),
    Terminate,
}