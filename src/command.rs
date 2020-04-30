use redis::Cmd;
use redis_event::cmd::Command;
use redis_event::cmd::keys::ORDER;
use redis_event::cmd::lists::POSITION;
use redis_event::cmd::sorted_sets::AGGREGATE;
use redis_event::cmd::strings::{ExistType, ExpireType, Op, Operation, Overflow};
use redis_event::EventHandler;
use redis_event::rdb::Object;

pub trait CommandConverter {
    fn handle_rdb(&mut self, rdb: Object) -> Option<Cmd> {
        match rdb {
            Object::String(kv) => {
                let mut cmd = redis::cmd("set");
                cmd.arg(kv.key).arg(kv.value);
                return Some(cmd);
            }
            Object::List(list) => {
                let mut cmd = redis::cmd("rpush");
                cmd.arg(list.key);
                for val in list.values {
                    cmd.arg(val.as_slice());
                }
                return Some(cmd);
            }
            Object::Set(set) => {
                let mut cmd = redis::cmd("sadd");
                cmd.arg(set.key);
                for member in set.members {
                    cmd.arg(member.as_slice());
                }
                return Some(cmd);
            }
            Object::SortedSet(sorted_set) => {
                let mut cmd = redis::cmd("zadd");
                cmd.arg(sorted_set.key);
                for item in sorted_set.items {
                    cmd.arg(item.score).arg(item.member.as_slice());
                }
                return Some(cmd);
            }
            Object::Hash(hash) => {
                let mut cmd = redis::cmd("hmset");
                cmd.arg(hash.key);
                for field in hash.fields {
                    cmd.arg(field.name.as_slice()).arg(field.value.as_slice());
                }
                return Some(cmd);
            }
            _ => return None
        };
    }
    
    fn handle_aof(&mut self, cmd: Command) -> Option<Cmd> {
        match cmd {
            Command::APPEND(append) => {
                let mut cmd = redis::cmd("APPEND");
                cmd.arg(append.key).arg(append.value);
                return Some(cmd);
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
                return Some(cmd);
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
                return Some(cmd);
            }
            Command::BRPOPLPUSH(brpoplpush) => {
                let mut cmd = redis::cmd("BRPOPLPUSH");
                cmd.arg(brpoplpush.source)
                    .arg(brpoplpush.destination)
                    .arg(brpoplpush.timeout);
                return Some(cmd);
            }
            Command::DECR(decr) => {
                let mut cmd = redis::cmd("DECR");
                cmd.arg(decr.key);
                return Some(cmd);
            }
            Command::DECRBY(decrby) => {
                let mut cmd = redis::cmd("DECRBY");
                cmd.arg(decrby.key).arg(decrby.decrement);
                return Some(cmd);
            }
            Command::DEL(del) => {
                let mut cmd = redis::cmd("DEL");
                for key in &del.keys {
                    cmd.arg(key.as_slice());
                }
                return Some(cmd);
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
                return Some(cmd);
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
                return Some(cmd);
            }
            Command::EXPIRE(expire) => {
                let mut cmd = redis::cmd("EXPIRE");
                cmd.arg(expire.key).arg(expire.seconds);
                return Some(cmd);
            }
            Command::EXPIREAT(expireat) => {
                let mut cmd = redis::cmd("EXPIREAT");
                cmd.arg(expireat.key).arg(expireat.timestamp);
                return Some(cmd);
            }
            Command::EXEC => {
                let cmd = redis::cmd("EXEC");
                return Some(cmd);
            }
            Command::FLUSHALL(flushall) => {
                let mut cmd = redis::cmd("FLUSHALL");
                if flushall._async.is_some() {
                    cmd.arg("ASYNC");
                }
                return Some(cmd);
            }
            Command::FLUSHDB(flushdb) => {
                let mut cmd = redis::cmd("FLUSHDB");
                if flushdb._async.is_some() {
                    cmd.arg("ASYNC");
                }
                return Some(cmd);
            }
            Command::GETSET(getset) => {
                let mut cmd = redis::cmd("GETSET");
                cmd.arg(getset.key).arg(getset.value);
                return Some(cmd);
            }
            Command::HDEL(hdel) => {
                let mut cmd = redis::cmd("HDEL");
                cmd.arg(hdel.key);
                for field in &hdel.fields {
                    cmd.arg(*field);
                }
                return Some(cmd);
            }
            Command::HINCRBY(hincrby) => {
                let mut cmd = redis::cmd("HINCRBY");
                cmd.arg(hincrby.key).arg(hincrby.field).arg(hincrby.increment);
                return Some(cmd);
            }
            Command::HMSET(hmset) => {
                let mut cmd = redis::cmd("HMSET");
                cmd.arg(hmset.key);
                for field in &hmset.fields {
                    cmd.arg(field.name).arg(field.value);
                }
                return Some(cmd);
            }
            Command::HSET(hset) => {
                let mut cmd = redis::cmd("HSET");
                cmd.arg(hset.key);
                for field in &hset.fields {
                    cmd.arg(field.name).arg(field.value);
                }
                return Some(cmd);
            }
            Command::HSETNX(hsetnx) => {
                let mut cmd = redis::cmd("HSETNX");
                cmd.arg(hsetnx.key).arg(hsetnx.field).arg(hsetnx.value);
                return Some(cmd);
            }
            Command::INCR(incr) => {
                let mut cmd = redis::cmd("INCR");
                cmd.arg(incr.key);
                return Some(cmd);
            }
            Command::INCRBY(incrby) => {
                let mut cmd = redis::cmd("INCRBY");
                cmd.arg(incrby.key).arg(incrby.increment);
                return Some(cmd);
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
                return Some(cmd);
            }
            Command::LPOP(lpop) => {
                let mut cmd = redis::cmd("LPOP");
                cmd.arg(lpop.key);
                return Some(cmd);
            }
            Command::LPUSH(lpush) => {
                let mut cmd = redis::cmd("LPUSH");
                cmd.arg(lpush.key);
                for element in &lpush.elements {
                    cmd.arg(*element);
                }
                return Some(cmd);
            }
            Command::LPUSHX(lpushx) => {
                let mut cmd = redis::cmd("LPUSHX");
                cmd.arg(lpushx.key);
                for element in &lpushx.elements {
                    cmd.arg(*element);
                }
                return Some(cmd);
            }
            Command::LREM(lrem) => {
                let mut cmd = redis::cmd("LREM");
                cmd.arg(lrem.key).arg(lrem.count).arg(lrem.element);
                return Some(cmd);
            }
            Command::LSET(lset) => {
                let mut cmd = redis::cmd("LSET");
                cmd.arg(lset.key).arg(lset.index).arg(lset.element);
                return Some(cmd);
            }
            Command::LTRIM(ltrim) => {
                let mut cmd = redis::cmd("LTRIM");
                cmd.arg(ltrim.key).arg(ltrim.start).arg(ltrim.stop);
                return Some(cmd);
            }
            Command::MOVE(_move) => {
                let mut cmd = redis::cmd("MOVE");
                cmd.arg(_move.key).arg(_move.db);
                return Some(cmd);
            }
            Command::MSET(mset) => {
                let mut cmd = redis::cmd("MSET");
                for kv in &mset.key_values {
                    cmd.arg(kv.key).arg(kv.value);
                }
                return Some(cmd);
            }
            Command::MSETNX(msetnx) => {
                let mut cmd = redis::cmd("MSETNX");
                for kv in &msetnx.key_values {
                    cmd.arg(kv.key).arg(kv.value);
                }
                return Some(cmd);
            }
            Command::MULTI => {
                let cmd = redis::cmd("MULTI");
                return Some(cmd);
            }
            Command::PERSIST(persist) => {
                let mut cmd = redis::cmd("PERSIST");
                cmd.arg(persist.key);
                return Some(cmd);
            }
            Command::PEXPIRE(pexpire) => {
                let mut cmd = redis::cmd("PEXPIRE");
                cmd.arg(pexpire.milliseconds);
                return Some(cmd);
            }
            Command::PEXPIREAT(pexpireat) => {
                let mut cmd = redis::cmd("PEXPIREAT");
                cmd.arg(pexpireat.mill_timestamp);
                return Some(cmd);
            }
            Command::PFADD(pfadd) => {
                let mut cmd = redis::cmd("PFADD");
                cmd.arg(pfadd.key);
                for element in &pfadd.elements {
                    cmd.arg(*element);
                }
                return Some(cmd);
            }
            Command::PFCOUNT(pfcount) => {
                let mut cmd = redis::cmd("PFCOUNT");
                for key in &pfcount.keys {
                    cmd.arg(*key);
                }
                return Some(cmd);
            }
            Command::PFMERGE(pfmerge) => {
                let mut cmd = redis::cmd("PFMERGE");
                cmd.arg(pfmerge.dest_key);
                for key in &pfmerge.source_keys {
                    cmd.arg(*key);
                }
                return Some(cmd);
            }
            Command::PSETEX(psetex) => {
                let mut cmd = redis::cmd("PSETEX");
                cmd.arg(psetex.key).arg(psetex.milliseconds).arg(psetex.value);
                return Some(cmd);
            }
            Command::PUBLISH(publish) => {
                let mut cmd = redis::cmd("PUBLISH");
                cmd.arg(publish.channel).arg(publish.message);
                return Some(cmd);
            }
            Command::RENAME(rename) => {
                let mut cmd = redis::cmd("RENAME");
                cmd.arg(rename.key).arg(rename.new_key);
                return Some(cmd);
            }
            Command::RENAMENX(renamenx) => {
                let mut cmd = redis::cmd("RENAMENX");
                cmd.arg(renamenx.key).arg(renamenx.new_key);
                return Some(cmd);
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
                return Some(cmd);
            }
            Command::RPOP(rpop) => {
                let mut cmd = redis::cmd("RPOP");
                cmd.arg(rpop.key);
                return Some(cmd);
            }
            Command::RPOPLPUSH(rpoplpush) => {
                let mut cmd = redis::cmd("RPOPLPUSH");
                cmd.arg(rpoplpush.source).arg(rpoplpush.destination);
                return Some(cmd);
            }
            Command::RPUSH(rpush) => {
                let mut cmd = redis::cmd("RPUSH");
                cmd.arg(rpush.key);
                for element in &rpush.elements {
                    cmd.arg(*element);
                }
                return Some(cmd);
            }
            Command::RPUSHX(rpushx) => {
                let mut cmd = redis::cmd("RPUSHX");
                cmd.arg(rpushx.key);
                for element in &rpushx.elements {
                    cmd.arg(*element);
                }
                return Some(cmd);
            }
            Command::SADD(sadd) => {
                let mut cmd = redis::cmd("SADD");
                cmd.arg(sadd.key);
                for member in &sadd.members {
                    cmd.arg(*member);
                }
                return Some(cmd);
            }
            Command::SCRIPTFLUSH => {
                let mut cmd = redis::cmd("SCRIPT");
                cmd.arg("FLUSH");
                return Some(cmd);
            }
            Command::SCRIPTLOAD(scriptload) => {
                let mut cmd = redis::cmd("SCRIPT");
                cmd.arg("LOAD").arg(scriptload.script);
                return Some(cmd);
            }
            Command::SDIFFSTORE(sdiffstore) => {
                let mut cmd = redis::cmd("SDIFFSTORE");
                cmd.arg(sdiffstore.destination);
                for key in &sdiffstore.keys {
                    cmd.arg(*key);
                }
                return Some(cmd);
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
                return Some(cmd);
            }
            Command::SETBIT(setbit) => {
                let mut cmd = redis::cmd("SETBIT");
                cmd.arg(setbit.key).arg(setbit.offset).arg(setbit.value);
                return Some(cmd);
            }
            Command::SETEX(setex) => {
                let mut cmd = redis::cmd("SETEX");
                cmd.arg(setex.key).arg(setex.seconds).arg(setex.value);
                return Some(cmd);
            }
            Command::SETNX(setnx) => {
                let mut cmd = redis::cmd("SETNX");
                cmd.arg(setnx.key).arg(setnx.value);
                return Some(cmd);
            }
            Command::SELECT(select) => {
                let mut cmd = redis::cmd("SELECT");
                cmd.arg(select.db);
                return Some(cmd);
            }
            Command::SETRANGE(setrange) => {
                let mut cmd = redis::cmd("SETRANGE");
                cmd.arg(setrange.key).arg(setrange.offset).arg(setrange.value);
                return Some(cmd);
            }
            Command::SINTERSTORE(sinterstore) => {
                let mut cmd = redis::cmd("SINTERSTORE");
                cmd.arg(sinterstore.destination);
                for key in &sinterstore.keys {
                    cmd.arg(*key);
                }
                return Some(cmd);
            }
            Command::SMOVE(smove) => {
                let mut cmd = redis::cmd("SMOVE");
                cmd.arg(smove.source).arg(smove.destination).arg(smove.member);
                return Some(cmd);
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
                return Some(cmd);
            }
            Command::SREM(srem) => {
                let mut cmd = redis::cmd("SREM");
                cmd.arg(srem.key);
                for member in &srem.members {
                    cmd.arg(*member);
                }
                return Some(cmd);
            }
            Command::SUNIONSTORE(sunion) => {
                let mut cmd = redis::cmd("SUNIONSTORE");
                cmd.arg(sunion.destination);
                for key in &sunion.keys {
                    cmd.arg(*key);
                }
                return Some(cmd);
            }
            Command::SWAPDB(swapdb) => {
                let mut cmd = redis::cmd("SWAPDB");
                cmd.arg(swapdb.index1).arg(swapdb.index2);
                return Some(cmd);
            }
            Command::UNLINK(unlink) => {
                let mut cmd = redis::cmd("UNLINK");
                for key in &unlink.keys {
                    cmd.arg(*key);
                }
                return Some(cmd);
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
                return Some(cmd);
            }
            Command::ZINCRBY(zincrby) => {
                let mut cmd = redis::cmd("ZINCRBY");
                cmd.arg(zincrby.key).arg(zincrby.increment).arg(zincrby.member);
                return Some(cmd);
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
                return Some(cmd);
            }
            Command::ZPOPMAX(zpopmax) => {
                let mut cmd = redis::cmd("ZPOPMAX");
                cmd.arg(zpopmax.key);
                if let Some(count) = zpopmax.count {
                    cmd.arg(count);
                }
                return Some(cmd);
            }
            Command::ZPOPMIN(zpopmin) => {
                let mut cmd = redis::cmd("ZPOPMIN");
                cmd.arg(zpopmin.key);
                if let Some(count) = zpopmin.count {
                    cmd.arg(count);
                }
                return Some(cmd);
            }
            Command::ZREM(zrem) => {
                let mut cmd = redis::cmd("ZREM");
                cmd.arg(zrem.key);
                for member in &zrem.members {
                    cmd.arg(*member);
                }
                return Some(cmd);
            }
            Command::ZREMRANGEBYLEX(zrem) => {
                let mut cmd = redis::cmd("ZREMRANGEBYLEX");
                cmd.arg(zrem.key).arg(zrem.min).arg(zrem.max);
                return Some(cmd);
            }
            Command::ZREMRANGEBYRANK(zrem) => {
                let mut cmd = redis::cmd("ZREMRANGEBYRANK");
                cmd.arg(zrem.key).arg(zrem.start).arg(zrem.stop);
                return Some(cmd);
            }
            Command::ZREMRANGEBYSCORE(zrem) => {
                let mut cmd = redis::cmd("ZREMRANGEBYSCORE");
                cmd.arg(zrem.key).arg(zrem.min).arg(zrem.max);
                return Some(cmd);
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
                return Some(cmd);
            }
            Command::Other(raw_cmd) => {
                let mut cmd = redis::cmd(&raw_cmd.name);
                for arg in raw_cmd.args {
                    cmd.arg(arg);
                }
                return Some(cmd);
            }
        }
    }
}

impl<R: EventHandler + ?Sized> CommandConverter for R {}
