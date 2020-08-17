use std::error;
use std::ops::DerefMut;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::mpsc::Receiver;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use log::{error, info};
use r2d2_redis::r2d2::{CustomizeConnection, HandleError};
use r2d2_redis::redis::{Connection, IntoConnectionInfo};
use r2d2_redis::{r2d2, RedisConnectionManager};
use scheduled_thread_pool::ScheduledThreadPool;

pub(crate) struct Worker {
    pub(crate) thread: Option<thread::JoinHandle<()>>,
}

pub(crate) enum Message {
    Cmd(redis::Cmd),
    SwapDb(i64),
    Terminate,
}

pub(crate) fn new_worker(
    target: String, receiver: Receiver<Message>, name: &str, batch_size: i32, flush_interval: u64,
    control_flag: Arc<AtomicBool>,
) -> thread::JoinHandle<()> {
    let builder = thread::Builder::new().name(name.into());
    let worker = builder
        .spawn(move || {
            let handle = thread::current();
            let t_name = handle.name().unwrap();
            info!(target: t_name, "Worker thread started");
            let conn_info = target
                .as_str()
                .into_connection_info()
                .expect("解析Target Redis地址失败");
            let db: Arc<AtomicI64> = Arc::new(AtomicI64::new(conn_info.db));

            let manager = RedisConnectionManager::new(target).unwrap();
            let pool = r2d2::Pool::builder()
                .max_size(1)
                .thread_pool(Arc::new(ScheduledThreadPool::with_name("r2d2-worker-{}", 1)))
                .error_handler(Box::new(ConnectionErrorHandler { control_flag }))
                .connection_customizer(Box::new(ConnectionCustomizer { db: Arc::clone(&db) }))
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
                            db.load(Ordering::Relaxed);
                            count += 1;
                        }
                        Ok(Message::Terminate) => {
                            shutdown = true;
                        }
                        Ok(Message::SwapDb(_db)) => {
                            db.store(_db, Ordering::SeqCst);
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
            panic!("Extension error. This error may be caused by ACL, please check your Redis's ACL config.")
        } else {
            error!("{}", error);
        }
    }
}

#[derive(Debug)]
struct ConnectionCustomizer {
    db: Arc<AtomicI64>,
}

impl CustomizeConnection<Connection, r2d2_redis::Error> for ConnectionCustomizer {
    fn on_acquire(&self, conn: &mut Connection) -> Result<(), r2d2_redis::Error> {
        let db = self.db.load(Ordering::Relaxed);
        match redis::cmd("SELECT").arg(db).query(conn) {
            Ok(()) => info!("db切换至{}", db),
            Err(e) => {
                error!("切换db失败: {}", e);
                return Err(r2d2_redis::Error::Other(e));
            }
        }
        Ok(())
    }
}
