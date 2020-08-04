use redis::Cmd;
use redis_event::cmd::keys::ORDER;
use redis_event::cmd::lists::POSITION;
use redis_event::cmd::sorted_sets::AGGREGATE;
use redis_event::cmd::strings::{ExistType, ExpireType, Op, Operation, Overflow};
use redis_event::cmd::Command;
use redis_event::rdb;
use redis_event::rdb::Object;

pub trait CommandConverter {
    fn handle_rdb(&mut self, rdb: Object) {
        match rdb {
            Object::String(kv) => {
                let mut cmd = redis::cmd("set");
                cmd.arg(kv.key).arg(kv.value);
                self.execute(cmd, None);
                self.handle_expire(kv.key, &kv.meta.expire);
            }
            Object::List(list) => {
                let mut cmd = redis::cmd("rpush");
                cmd.arg(list.key);
                for val in list.values {
                    cmd.arg(val.as_slice());
                }
                self.execute(cmd, None);
                self.handle_expire(list.key, &list.meta.expire);
            }
            Object::Set(set) => {
                let mut cmd = redis::cmd("sadd");
                cmd.arg(set.key);
                for member in set.members {
                    cmd.arg(member.as_slice());
                }
                self.execute(cmd, None);
                self.handle_expire(set.key, &set.meta.expire);
            }
            Object::SortedSet(sorted_set) => {
                let mut cmd = redis::cmd("zadd");
                cmd.arg(sorted_set.key);
                for item in sorted_set.items {
                    cmd.arg(item.score).arg(item.member.as_slice());
                }
                self.execute(cmd, None);
                self.handle_expire(sorted_set.key, &sorted_set.meta.expire);
            }
            Object::Hash(hash) => {
                let mut cmd = redis::cmd("hmset");
                cmd.arg(hash.key);
                for field in hash.fields {
                    cmd.arg(field.name.as_slice()).arg(field.value.as_slice());
                }
                self.execute(cmd, None);
                self.handle_expire(hash.key, &hash.meta.expire);
            }
            Object::Stream(key, stream) => {
                for (id, entry) in stream.entries {
                    let mut cmd = redis::cmd("XADD");
                    cmd.arg(key.as_slice());
                    cmd.arg(id.to_string());
                    for (field, value) in entry.fields {
                        cmd.arg(field).arg(value);
                    }
                    self.execute(cmd, Some(key.as_slice()));
                }
                for group in stream.groups {
                    let mut cmd = redis::cmd("XGROUP");
                    cmd.arg("CREATE")
                        .arg(key.as_slice())
                        .arg(group.name)
                        .arg(group.last_id.to_string());
                    self.execute(cmd, Some(key.as_slice()));
                }
                self.handle_expire(key.as_slice(), &stream.meta.expire);
            }
            _ => {}
        };
    }

    fn handle_aof(&mut self, cmd: Command) {
        match cmd {
            Command::APPEND(append) => {
                let mut cmd = redis::cmd("APPEND");
                cmd.arg(append.key).arg(append.value);
                self.execute(cmd, None);
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
                                cmd.arg("INCRBY")
                                    .arg(incrby._type)
                                    .arg(incrby.offset)
                                    .arg(incrby.increment);
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
                self.execute(cmd, None);
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
                self.execute(cmd, None);
            }
            Command::BRPOPLPUSH(brpoplpush) => {
                let mut cmd = redis::cmd("BRPOPLPUSH");
                cmd.arg(brpoplpush.source)
                    .arg(brpoplpush.destination)
                    .arg(brpoplpush.timeout);
                self.execute(cmd, None);
            }
            Command::DECR(decr) => {
                let mut cmd = redis::cmd("DECR");
                cmd.arg(decr.key);
                self.execute(cmd, None);
            }
            Command::DECRBY(decrby) => {
                let mut cmd = redis::cmd("DECRBY");
                cmd.arg(decrby.key).arg(decrby.decrement);
                self.execute(cmd, None);
            }
            Command::DEL(del) => {
                let mut cmd = redis::cmd("DEL");
                for key in &del.keys {
                    cmd.arg(key.as_slice());
                }
                self.execute(cmd, None);
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
                self.execute(cmd, None);
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
                self.execute(cmd, None);
            }
            Command::EXPIRE(expire) => {
                let mut cmd = redis::cmd("EXPIRE");
                cmd.arg(expire.key).arg(expire.seconds);
                self.execute(cmd, None);
            }
            Command::EXPIREAT(expireat) => {
                let mut cmd = redis::cmd("EXPIREAT");
                cmd.arg(expireat.key).arg(expireat.timestamp);
                self.execute(cmd, None);
            }
            Command::EXEC => {
                let cmd = redis::cmd("EXEC");
                self.execute(cmd, None);
            }
            Command::FLUSHALL(flushall) => {
                let mut cmd = redis::cmd("FLUSHALL");
                if flushall._async.is_some() {
                    cmd.arg("ASYNC");
                }
                self.execute(cmd, None);
            }
            Command::FLUSHDB(flushdb) => {
                let mut cmd = redis::cmd("FLUSHDB");
                if flushdb._async.is_some() {
                    cmd.arg("ASYNC");
                }
                self.execute(cmd, None);
            }
            Command::GETSET(getset) => {
                let mut cmd = redis::cmd("GETSET");
                cmd.arg(getset.key).arg(getset.value);
                self.execute(cmd, None);
            }
            Command::HDEL(hdel) => {
                let mut cmd = redis::cmd("HDEL");
                cmd.arg(hdel.key);
                for field in &hdel.fields {
                    cmd.arg(*field);
                }
                self.execute(cmd, None);
            }
            Command::HINCRBY(hincrby) => {
                let mut cmd = redis::cmd("HINCRBY");
                cmd.arg(hincrby.key)
                    .arg(hincrby.field)
                    .arg(hincrby.increment);
                self.execute(cmd, None);
            }
            Command::HMSET(hmset) => {
                let mut cmd = redis::cmd("HMSET");
                cmd.arg(hmset.key);
                for field in &hmset.fields {
                    cmd.arg(field.name).arg(field.value);
                }
                self.execute(cmd, None);
            }
            Command::HSET(hset) => {
                let mut cmd = redis::cmd("HSET");
                cmd.arg(hset.key);
                for field in &hset.fields {
                    cmd.arg(field.name).arg(field.value);
                }
                self.execute(cmd, None);
            }
            Command::HSETNX(hsetnx) => {
                let mut cmd = redis::cmd("HSETNX");
                cmd.arg(hsetnx.key).arg(hsetnx.field).arg(hsetnx.value);
                self.execute(cmd, None);
            }
            Command::INCR(incr) => {
                let mut cmd = redis::cmd("INCR");
                cmd.arg(incr.key);
                self.execute(cmd, None);
            }
            Command::INCRBY(incrby) => {
                let mut cmd = redis::cmd("INCRBY");
                cmd.arg(incrby.key).arg(incrby.increment);
                self.execute(cmd, None);
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
                self.execute(cmd, None);
            }
            Command::LPOP(lpop) => {
                let mut cmd = redis::cmd("LPOP");
                cmd.arg(lpop.key);
                self.execute(cmd, None);
            }
            Command::LPUSH(lpush) => {
                let mut cmd = redis::cmd("LPUSH");
                cmd.arg(lpush.key);
                for element in &lpush.elements {
                    cmd.arg(*element);
                }
                self.execute(cmd, None);
            }
            Command::LPUSHX(lpushx) => {
                let mut cmd = redis::cmd("LPUSHX");
                cmd.arg(lpushx.key);
                for element in &lpushx.elements {
                    cmd.arg(*element);
                }
                self.execute(cmd, None);
            }
            Command::LREM(lrem) => {
                let mut cmd = redis::cmd("LREM");
                cmd.arg(lrem.key).arg(lrem.count).arg(lrem.element);
                self.execute(cmd, None);
            }
            Command::LSET(lset) => {
                let mut cmd = redis::cmd("LSET");
                cmd.arg(lset.key).arg(lset.index).arg(lset.element);
                self.execute(cmd, None);
            }
            Command::LTRIM(ltrim) => {
                let mut cmd = redis::cmd("LTRIM");
                cmd.arg(ltrim.key).arg(ltrim.start).arg(ltrim.stop);
                self.execute(cmd, None);
            }
            Command::MOVE(_move) => {
                let mut cmd = redis::cmd("MOVE");
                cmd.arg(_move.key).arg(_move.db);
                self.execute(cmd, None);
            }
            Command::MSET(mset) => {
                let mut cmd = redis::cmd("MSET");
                for kv in &mset.key_values {
                    cmd.arg(kv.key).arg(kv.value);
                }
                self.execute(cmd, None);
            }
            Command::MSETNX(msetnx) => {
                let mut cmd = redis::cmd("MSETNX");
                for kv in &msetnx.key_values {
                    cmd.arg(kv.key).arg(kv.value);
                }
                self.execute(cmd, None);
            }
            Command::MULTI => {
                let cmd = redis::cmd("MULTI");
                self.execute(cmd, None);
            }
            Command::PERSIST(persist) => {
                let mut cmd = redis::cmd("PERSIST");
                cmd.arg(persist.key);
                self.execute(cmd, None);
            }
            Command::PEXPIRE(pexpire) => {
                let mut cmd = redis::cmd("PEXPIRE");
                cmd.arg(pexpire.milliseconds);
                self.execute(cmd, None);
            }
            Command::PEXPIREAT(pexpireat) => {
                let mut cmd = redis::cmd("PEXPIREAT");
                cmd.arg(pexpireat.mill_timestamp);
                self.execute(cmd, None);
            }
            Command::PFADD(pfadd) => {
                let mut cmd = redis::cmd("PFADD");
                cmd.arg(pfadd.key);
                for element in &pfadd.elements {
                    cmd.arg(*element);
                }
                self.execute(cmd, None);
            }
            Command::PFCOUNT(pfcount) => {
                let mut cmd = redis::cmd("PFCOUNT");
                for key in &pfcount.keys {
                    cmd.arg(*key);
                }
                self.execute(cmd, None);
            }
            Command::PFMERGE(pfmerge) => {
                let mut cmd = redis::cmd("PFMERGE");
                cmd.arg(pfmerge.dest_key);
                for key in &pfmerge.source_keys {
                    cmd.arg(*key);
                }
                self.execute(cmd, None);
            }
            Command::PSETEX(psetex) => {
                let mut cmd = redis::cmd("PSETEX");
                cmd.arg(psetex.key)
                    .arg(psetex.milliseconds)
                    .arg(psetex.value);
                self.execute(cmd, None);
            }
            Command::PUBLISH(publish) => {
                let mut cmd = redis::cmd("PUBLISH");
                cmd.arg(publish.channel).arg(publish.message);
                self.execute(cmd, None);
            }
            Command::RENAME(rename) => {
                let mut cmd = redis::cmd("RENAME");
                cmd.arg(rename.key).arg(rename.new_key);
                self.execute(cmd, None);
            }
            Command::RENAMENX(renamenx) => {
                let mut cmd = redis::cmd("RENAMENX");
                cmd.arg(renamenx.key).arg(renamenx.new_key);
                self.execute(cmd, None);
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
                self.execute(cmd, None);
            }
            Command::RPOP(rpop) => {
                let mut cmd = redis::cmd("RPOP");
                cmd.arg(rpop.key);
                self.execute(cmd, None);
            }
            Command::RPOPLPUSH(rpoplpush) => {
                let mut cmd = redis::cmd("RPOPLPUSH");
                cmd.arg(rpoplpush.source).arg(rpoplpush.destination);
                self.execute(cmd, None);
            }
            Command::RPUSH(rpush) => {
                let mut cmd = redis::cmd("RPUSH");
                cmd.arg(rpush.key);
                for element in &rpush.elements {
                    cmd.arg(*element);
                }
                self.execute(cmd, None);
            }
            Command::RPUSHX(rpushx) => {
                let mut cmd = redis::cmd("RPUSHX");
                cmd.arg(rpushx.key);
                for element in &rpushx.elements {
                    cmd.arg(*element);
                }
                self.execute(cmd, None);
            }
            Command::SADD(sadd) => {
                let mut cmd = redis::cmd("SADD");
                cmd.arg(sadd.key);
                for member in &sadd.members {
                    cmd.arg(*member);
                }
                self.execute(cmd, None);
            }
            Command::SCRIPTFLUSH => {
                let mut cmd = redis::cmd("SCRIPT");
                cmd.arg("FLUSH");
                self.execute(cmd, None);
            }
            Command::SCRIPTLOAD(scriptload) => {
                let mut cmd = redis::cmd("SCRIPT");
                cmd.arg("LOAD").arg(scriptload.script);
                self.execute(cmd, None);
            }
            Command::SDIFFSTORE(sdiffstore) => {
                let mut cmd = redis::cmd("SDIFFSTORE");
                cmd.arg(sdiffstore.destination);
                for key in &sdiffstore.keys {
                    cmd.arg(*key);
                }
                self.execute(cmd, None);
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
                if set.keep_ttl.as_ref().is_some() {
                    cmd.arg("KEEPTTL");
                }
                self.execute(cmd, None);
            }
            Command::SETBIT(setbit) => {
                let mut cmd = redis::cmd("SETBIT");
                cmd.arg(setbit.key).arg(setbit.offset).arg(setbit.value);
                self.execute(cmd, None);
            }
            Command::SETEX(setex) => {
                let mut cmd = redis::cmd("SETEX");
                cmd.arg(setex.key).arg(setex.seconds).arg(setex.value);
                self.execute(cmd, None);
            }
            Command::SETNX(setnx) => {
                let mut cmd = redis::cmd("SETNX");
                cmd.arg(setnx.key).arg(setnx.value);
                self.execute(cmd, None);
            }
            Command::SELECT(select) => {
                let mut cmd = redis::cmd("SELECT");
                cmd.arg(select.db);
                self.execute(cmd, None);
            }
            Command::SETRANGE(setrange) => {
                let mut cmd = redis::cmd("SETRANGE");
                cmd.arg(setrange.key)
                    .arg(setrange.offset)
                    .arg(setrange.value);
                self.execute(cmd, None);
            }
            Command::SINTERSTORE(sinterstore) => {
                let mut cmd = redis::cmd("SINTERSTORE");
                cmd.arg(sinterstore.destination);
                for key in &sinterstore.keys {
                    cmd.arg(*key);
                }
                self.execute(cmd, None);
            }
            Command::SMOVE(smove) => {
                let mut cmd = redis::cmd("SMOVE");
                cmd.arg(smove.source)
                    .arg(smove.destination)
                    .arg(smove.member);
                self.execute(cmd, None);
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
                self.execute(cmd, None);
            }
            Command::SREM(srem) => {
                let mut cmd = redis::cmd("SREM");
                cmd.arg(srem.key);
                for member in &srem.members {
                    cmd.arg(*member);
                }
                self.execute(cmd, None);
            }
            Command::SUNIONSTORE(sunion) => {
                let mut cmd = redis::cmd("SUNIONSTORE");
                cmd.arg(sunion.destination);
                for key in &sunion.keys {
                    cmd.arg(*key);
                }
                self.execute(cmd, None);
            }
            Command::SWAPDB(swapdb) => {
                let mut cmd = redis::cmd("SWAPDB");
                cmd.arg(swapdb.index1).arg(swapdb.index2);
                self.execute(cmd, None);
            }
            Command::UNLINK(unlink) => {
                let mut cmd = redis::cmd("UNLINK");
                for key in &unlink.keys {
                    cmd.arg(*key);
                }
                self.execute(cmd, None);
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
                self.execute(cmd, None);
            }
            Command::ZINCRBY(zincrby) => {
                let mut cmd = redis::cmd("ZINCRBY");
                cmd.arg(zincrby.key)
                    .arg(zincrby.increment)
                    .arg(zincrby.member);
                self.execute(cmd, None);
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
                self.execute(cmd, None);
            }
            Command::ZPOPMAX(zpopmax) => {
                let mut cmd = redis::cmd("ZPOPMAX");
                cmd.arg(zpopmax.key);
                if let Some(count) = zpopmax.count {
                    cmd.arg(count);
                }
                self.execute(cmd, None);
            }
            Command::ZPOPMIN(zpopmin) => {
                let mut cmd = redis::cmd("ZPOPMIN");
                cmd.arg(zpopmin.key);
                if let Some(count) = zpopmin.count {
                    cmd.arg(count);
                }
                self.execute(cmd, None);
            }
            Command::ZREM(zrem) => {
                let mut cmd = redis::cmd("ZREM");
                cmd.arg(zrem.key);
                for member in &zrem.members {
                    cmd.arg(*member);
                }
                self.execute(cmd, None);
            }
            Command::ZREMRANGEBYLEX(zrem) => {
                let mut cmd = redis::cmd("ZREMRANGEBYLEX");
                cmd.arg(zrem.key).arg(zrem.min).arg(zrem.max);
                self.execute(cmd, None);
            }
            Command::ZREMRANGEBYRANK(zrem) => {
                let mut cmd = redis::cmd("ZREMRANGEBYRANK");
                cmd.arg(zrem.key).arg(zrem.start).arg(zrem.stop);
                self.execute(cmd, None);
            }
            Command::ZREMRANGEBYSCORE(zrem) => {
                let mut cmd = redis::cmd("ZREMRANGEBYSCORE");
                cmd.arg(zrem.key).arg(zrem.min).arg(zrem.max);
                self.execute(cmd, None);
            }
            Command::ZUNIONSTORE(zunion) => {
                let mut cmd = redis::cmd("ZUNIONSTORE");
                cmd.arg(zunion.destination)
                    .arg(zunion.destination)
                    .arg(zunion.num_keys);
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
                self.execute(cmd, None);
            }
            Command::Other(raw_cmd) => {
                let mut cmd = redis::cmd(&raw_cmd.name);
                for arg in raw_cmd.args {
                    cmd.arg(arg);
                }
                self.execute(cmd, None);
            }
            Command::XACK(xack) => {
                let mut cmd = redis::cmd("XACK");
                cmd.arg(xack.key).arg(xack.group);
                for id in &xack.ids {
                    cmd.arg(id.as_slice());
                }
                self.execute(cmd, None);
            }
            Command::XADD(xadd) => {
                let mut cmd = redis::cmd("XADD");
                cmd.arg(xadd.key).arg(xadd.id);
                for field in &xadd.fields {
                    cmd.arg(field.name).arg(field.value);
                }
                self.execute(cmd, None);
            }
            Command::XCLAIM(xclaim) => {
                let mut cmd = redis::cmd("XCLAIM");
                cmd.arg(xclaim.key)
                    .arg(xclaim.group)
                    .arg(xclaim.consumer)
                    .arg(xclaim.min_idle_time);
                for id in &xclaim.ids {
                    cmd.arg(id.as_slice());
                }
                if let Some(idle) = xclaim.idle {
                    cmd.arg("IDLE").arg(idle.as_slice());
                }
                if let Some(time) = xclaim.time {
                    cmd.arg("TIME").arg(time.as_slice());
                }
                if let Some(retry_count) = xclaim.retry_count {
                    cmd.arg("RETRYCOUNT").arg(retry_count.as_slice());
                }
                if let Some(_) = xclaim.force {
                    cmd.arg("FORCE");
                }
                if let Some(_) = xclaim.just_id {
                    cmd.arg("JUSTID");
                }
                self.execute(cmd, None);
            }
            Command::XDEL(xdel) => {
                let mut cmd = redis::cmd("XDEL");
                cmd.arg(xdel.key);
                for id in &xdel.ids {
                    cmd.arg(id.as_slice());
                }
                self.execute(cmd, None);
            }
            Command::XGROUP(xgroup) => {
                let mut cmd = redis::cmd("XGROUP");
                if let Some(create) = &xgroup.create {
                    cmd.arg("CREATE")
                        .arg(create.key)
                        .arg(create.group_name)
                        .arg(create.id);
                }
                if let Some(set_id) = &xgroup.set_id {
                    cmd.arg("SETID")
                        .arg(set_id.key)
                        .arg(set_id.group_name)
                        .arg(set_id.id);
                }
                if let Some(destroy) = &xgroup.destroy {
                    cmd.arg("DESTROY").arg(destroy.key).arg(destroy.group_name);
                }
                if let Some(del_consumer) = &xgroup.del_consumer {
                    cmd.arg("DELCONSUMER")
                        .arg(del_consumer.key)
                        .arg(del_consumer.group_name)
                        .arg(del_consumer.consumer_name);
                }
                self.execute(cmd, None);
            }
            Command::XTRIM(xtrim) => {
                let mut cmd = redis::cmd("XTRIM");
                cmd.arg(xtrim.key).arg("MAXLEN");
                if xtrim.approximation {
                    cmd.arg("~");
                }
                cmd.arg(xtrim.count);
                self.execute(cmd, None);
            }
        }
    }

    fn handle_expire(&mut self, key: &[u8], expire: &Option<(rdb::ExpireType, i64)>) {
        if let Some((expire_type, ttl)) = expire {
            match expire_type {
                rdb::ExpireType::Second => {
                    let mut cmd = redis::cmd("EXPIREAT");
                    cmd.arg(key).arg(*ttl);
                    self.execute(cmd, Some(key));
                }
                rdb::ExpireType::Millisecond => {
                    let mut cmd = redis::cmd("PEXPIREAT");
                    cmd.arg(key).arg(*ttl);
                    self.execute(cmd, Some(key));
                }
            }
        }
    }

    fn execute(&mut self, cmd: Cmd, key: Option<&[u8]>);
}
