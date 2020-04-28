use std::ops::DerefMut;
use std::sync::Arc;
use std::sync::mpsc::Receiver;
use std::thread;
use std::time::{Duration, Instant};

use log::{error, info};
use r2d2_redis::{r2d2, RedisConnectionManager};
use scheduled_thread_pool::ScheduledThreadPool;

pub(crate) struct Worker {
    pub(crate) thread: Option<thread::JoinHandle<()>>
}

pub(crate) enum Message {
    Cmd(redis::Cmd),
    Terminate,
}

pub(crate) fn new_worker(target: String, receiver: Receiver<Message>, name: &str) -> thread::JoinHandle<()> {
    let builder = thread::Builder::new()
        .name(name.into());
    let worker = builder.spawn(move || {
        let handle = thread::current();
        let t_name = handle.name().unwrap();
        info!(target: t_name, "Worker thread started");
        let manager = RedisConnectionManager::new(target).unwrap();
        let pool = r2d2::Pool::builder()
            .max_size(1)
            .thread_pool(Arc::new(ScheduledThreadPool::with_name("r2d2-worker-{}", 1)))
            .build(manager)
            .unwrap();
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
                let mut conn = pool.get().unwrap();
                match pipeline.query(conn.deref_mut()) {
                    Err(err) => {
                        error!(target: t_name, "数据写入失败: {}", err);
                    }
                    Ok(()) => {
                        info!(target: t_name, "写入成功: {}", count);
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
        info!(target: t_name, "Worker thread terminated");
    }).unwrap();
    return worker;
}