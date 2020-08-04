use log::{error, info};
use r2d2_redis::r2d2::HandleError;
use r2d2_redis::{r2d2, RedisConnectionManager};
use scheduled_thread_pool::ScheduledThreadPool;
use std::error;
use std::ops::DerefMut;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::Receiver;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

pub(crate) struct Worker {
    pub(crate) thread: Option<thread::JoinHandle<()>>,
}

pub(crate) enum Message {
    Cmd(redis::Cmd),
    Terminate,
}

pub(crate) fn new_worker(
    target: String,
    receiver: Receiver<Message>,
    name: &str,
    batch_size: i32,
    flush_interval: u64,
    control_flag: Arc<AtomicBool>,
) -> thread::JoinHandle<()> {
    let builder = thread::Builder::new().name(name.into());
    let worker = builder
        .spawn(move || {
            let handle = thread::current();
            let t_name = handle.name().unwrap();
            info!(target: t_name, "Worker thread started");
            let manager = RedisConnectionManager::new(target).unwrap();
            let pool = r2d2::Pool::builder()
                .max_size(1)
                .thread_pool(Arc::new(ScheduledThreadPool::with_name(
                    "r2d2-worker-{}",
                    1,
                )))
                .error_handler(Box::new(ConnectionErrorHandler { control_flag }))
                .build(manager)
                .unwrap();
            let mut pipeline = redis::pipe();
            let mut count = 0;
            let mut timer = Instant::now();
            let interval = Duration::from_millis(flush_interval);
            let mut shutdown = false;
            loop {
                if (batch_size < 0) || (count < batch_size) {
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
                }
                let elapsed = timer.elapsed();
                if (elapsed.ge(&interval) || shutdown) && count > 0 {
                    match pool.get() {
                        Ok(mut conn) => {
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
                        Err(err) => {
                            error!(target: t_name, "{}", err);
                        }
                    }
                }
                if shutdown {
                    break;
                };
            }
            info!(target: t_name, "Worker thread terminated");
        })
        .unwrap();
    return worker;
}

#[derive(Debug)]
struct ConnectionErrorHandler {
    control_flag: Arc<AtomicBool>,
}

impl<E> HandleError<E> for ConnectionErrorHandler
where
    E: error::Error,
{
    fn handle_error(&self, error: E) {
        if error.to_string().eq("extension error") {
            self.control_flag.store(false, Ordering::Relaxed);
            panic!("Extension error. This error may be caused by ACL, please check whether there is a +ping rule in your Redis's ACL config.")
        } else {
            error!("{}", error);
        }
    }
}
