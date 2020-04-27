use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use log::info;
use crate::handler::Message;
use std::sync::mpsc::Receiver;

pub(crate) fn new_worker(target: String, running: Arc<AtomicBool>, receiver: Receiver<Message>) -> thread::JoinHandle<()> {
    let worker = thread::spawn(move || {
        info!("Worker thread started");
        let client = redis::Client::open(target).unwrap();
        let mut conn = match client.get_connection() {
            Ok(conn) => conn,
            Err(err) => {
                running.store(false, Ordering::SeqCst);
                panic!("{}", err)
            }
        };
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
                match pipeline.query(&mut conn) {
                    Err(err) => {
                        panic!("数据写入失败: {}", err);
                    }
                    Ok(()) => {
                        info!("写入成功: {}", count);
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
        info!("Worker thread terminated");
    });
    return worker;
}