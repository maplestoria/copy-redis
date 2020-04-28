extern crate ctrlc;
extern crate getopts;
extern crate r2d2_redis;

use std::{env, thread};
use std::cell::RefCell;
use std::collections::hash_map::DefaultHasher;
use std::fs;
use std::fs::File;
use std::hash::{Hash, Hasher};
use std::io;
use std::io::{Error, Read, Write};
use std::net::ToSocketAddrs;
use std::path::PathBuf;
use std::process::exit;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use getopts::Options;
use log::{error, info};
use redis::{ConnectionAddr, IntoConnectionInfo};
use redis_event::listener::standalone;
use redis_event::RedisListener;

mod handler;
mod sharding;
mod command;
mod worker;

fn main() {
    let args: Vec<String> = env::args().collect();
    let opt: Opt = parse_args(args);
    setup_logger(&opt.log_file).expect("logger设置失败");
    run(opt);
}

fn run(opt: Opt) {
    let source = opt.source.into_connection_info().expect("源Redis URI无效");
    let socket_addr;
    if let ConnectionAddr::Tcp(host, port) = source.addr.as_ref() {
        let addr = format!("{}:{}", host, port);
        let mut iter = addr.to_socket_addrs().expect("");
        socket_addr = iter.next().unwrap();
    } else {
        unimplemented!("Unix Domain Socket");
    }
    let source_addr = socket_addr.to_string();
    
    let read_timeout = if opt.read_timeout <= 0 {
        None
    } else {
        Option::Some(Duration::from_millis(opt.read_timeout))
    };
    
    let write_timeout = if opt.write_timeout <= 0 {
        None
    } else {
        Option::Some(Duration::from_millis(opt.write_timeout))
    };
    
    let mut config = redis_event::config::Config {
        is_discard_rdb: opt.discard_rdb,
        is_aof: opt.aof,
        addr: socket_addr,
        password: source.passwd.unwrap_or_default(),
        repl_id: "?".to_string(),
        repl_offset: -1,
        read_timeout,
        write_timeout,
    };
    
    if let Ok((repl_id, repl_offset)) = load_repl_meta(&source_addr) {
        info!("获取到PSYNC记录信息, id: {}, offset: {}", repl_id, repl_offset);
        config.repl_id = repl_id;
        config.repl_offset = repl_offset;
    }
    
    // 先关闭listener，因为listener在读取流中的数据时，是阻塞的，
    // 所以在接收到ctrl-c信号的时候，得再等一会，等redis master的数据来到(或者读取超时)，此时，程序才会继续运行，
    // 等命令被handler处理完之后，listener才能结束，而且handler的结束还必须在listener之后，要不然丢数据
    let listener_running = Arc::new(AtomicBool::new(true));
    let r1 = listener_running.clone();
    let r2 = listener_running.clone();
    ctrlc::set_handler(move || {
        info!("接收到Ctrl-C信号, 等待程序退出...");
        r1.store(false, Ordering::SeqCst);
    }).expect("Error setting Ctrl-C handler");
    
    let mut listener = standalone::new(config, listener_running);
    
    if opt.sharding || opt.cluster {
        if opt.sharding && opt.cluster { panic!("不能同时指定sharding与cluster") }
        if opt.sharding {
            let event_handler = sharding::new_sharded(opt.targets);
            listener.set_event_handler(Rc::new(RefCell::new(event_handler)));
        } else {
            let event_handler = handler::new_cluster(opt.targets, r2);
            listener.set_event_handler(Rc::new(RefCell::new(event_handler)));
        }
    } else {
        let event_handler = handler::new(opt.targets.get(0).unwrap().to_string());
        listener.set_event_handler(Rc::new(RefCell::new(event_handler)));
    }
    
    let mut retry_count = 0;
    while retry_count <= opt.retry {
        if let Err(error) = listener.start() {
            error!("连接到源Redis错误: {}", error.to_string());
            retry_count += 1;
            thread::sleep(Duration::from_millis(opt.retry_interval));
        } else {
            break;
        }
    }
    
    // 程序正常退出时，保存repl id和offset
    if let Err(err) = save_repl_meta(&source_addr, &listener.config.repl_id, listener.config.repl_offset) {
        error!("保存PSYNC信息失败:{}", err);
    }
}

fn load_repl_meta(source_addr: &str) -> io::Result<(String, i64)> {
    let mut s = DefaultHasher::new();
    source_addr.hash(&mut s);
    let hash = s.finish();
    let path = format!("{}/{}", METADATA, hash);
    let mut file = File::open(PathBuf::from(path))?;
    let mut buf = String::new();
    file.read_to_string(&mut buf)?;
    let vec: Vec<&str> = buf.split(",").collect();
    if vec.len() == 2 {
        let id = vec.get(0).unwrap();
        let offset = vec.get(1).unwrap();
        if let Ok(offset) = offset.parse::<i64>() {
            return Ok((id.to_string(), offset));
        }
    }
    Err(Error::new(io::ErrorKind::InvalidData, "未能获取到有效的PSYNC记录信息"))
}

fn save_repl_meta(source_addr: &str, id: &str, offset: i64) -> io::Result<()> {
    let mut s = DefaultHasher::new();
    source_addr.hash(&mut s);
    let hash = s.finish();
    let path = format!("{}/{}", METADATA, hash);
    if let Err(_) = fs::metadata(METADATA) {
        fs::create_dir(METADATA)?;
    }
    let mut file = File::create(PathBuf::from(path))?;
    let meta = format!("{},{}", id, offset);
    file.write(meta.as_bytes())?;
    file.flush()?;
    Ok(())
}

#[derive(Debug)]
struct Opt {
    source: String,
    targets: Vec<String>,
    discard_rdb: bool,
    aof: bool,
    log_file: Option<String>,
    read_timeout: u64,
    write_timeout: u64,
    retry: u8,
    retry_interval: u64,
    sharding: bool,
    cluster: bool,
}

const METADATA: &'static str = ".copy-redis";
const VERSION: &'static str = env!("CARGO_PKG_VERSION");

fn parse_args(args: Vec<String>) -> Opt {
    let mut opts = Options::new();
    opts.optopt("s", "source", "此Redis内的数据将复制到目的Redis中. URI格式形如: \"redis://[:password@]host:port\", 中括号及其内容可省略", "源Redis的URI");
    opts.optmulti("t", "target", "URI格式同上", "目的Redis的URI");
    opts.optflag("", "discard-rdb", "是否跳过整个RDB不进行复制, 默认为false, 复制完整的RDB");
    opts.optflag("", "aof", "是否需要处理AOF, 默认为false, 当RDB复制完后, 程序将终止");
    opts.optflag("", "sharding", "是否是shard模式");
    opts.optflag("", "cluster", "是否是cluster模式");
    opts.optopt("l", "log", "不指定此选项, 日志将输出至标准输出流", "日志输出文件");
    opts.optopt("", "read-timeout", "默认0, 永不超时", "读超时时间, 单位毫秒");
    opts.optopt("", "write-timeout", "默认0, 永不超时", "写超时时间, 单位毫秒");
    opts.optopt("r", "retry", "默认5次. 最多重试255次", "失败重试次数");
    opts.optopt("i", "retry-interval", "默认2000. 不可为0", "失败重试间隔时间, 单位毫秒");
    opts.optflag("h", "help", "输出帮助信息");
    opts.optflag("v", "version", "");
    
    let matches = match opts.parse(&args[1..]) {
        Ok(m) => m,
        Err(e) => {
            eprint!("Error: {}\r\n\r\n", e.to_string());
            print_usage(&opts);
            exit(1);
        }
    };
    
    if matches.opt_present("h") {
        print_usage(&opts);
        exit(0);
    };
    if matches.opts_present(&[String::from("v"), String::from("version")]) {
        println!("copy-redis {}", VERSION);
        exit(0);
    }
    
    let (source, targets) = if matches.opt_present("s") && matches.opt_present("t") {
        (matches.opt_str("s").unwrap(), matches.opt_strs("t"))
    } else {
        eprint!("Error: {}\r\n\r\n", "请指定source与target参数");
        print_usage(&opts);
        exit(1);
    };
    
    let discard_rdb = matches.opt_present("discard-rdb");
    let sharding = matches.opt_present("sharding");
    let cluster = matches.opt_present("cluster");
    let aof = matches.opt_present("aof");
    let log_file = matches.opt_str("l");
    
    let mut read_timeout = 0;
    if let Some(str) = matches.opt_str("read-timeout") {
        read_timeout = str.parse::<u64>().expect("超时时间应为有效的数字");
    }
    
    let mut write_timeout = 0;
    if let Some(str) = matches.opt_str("write-timeout") {
        write_timeout = str.parse::<u64>().expect("超时时间应为有效的数字");
    }
    
    let mut retry = 5;
    if let Some(str) = matches.opt_str("retry") {
        retry = str.parse::<u8>().expect("重试次数应为有效的数字");
        if retry == 0 {
            retry = 1;
        }
    }
    
    let retry_interval = 2000;
    if let Some(str) = matches.opt_str("retry-interval") {
        write_timeout = str.parse::<u64>().expect("重试间隔时间应为有效的数字");
    }
    
    return Opt {
        source,
        targets,
        discard_rdb,
        aof,
        log_file,
        read_timeout,
        write_timeout,
        sharding,
        cluster,
        retry,
        retry_interval,
    };
}

fn print_usage(opts: &Options) {
    let brief = format!("Usage: copy-redis [options]");
    print!("{}", opts.usage(&brief));
}

fn setup_logger(log_file: &Option<String>) -> Result<(), fern::InitError> {
    let mut base_config = fern::Dispatch::new();
    
    base_config = base_config.level(log::LevelFilter::Info);
    
    let log_format = fern::Dispatch::new()
        .format(|out, message, record| {
            out.finish(format_args!(
                "{} {} {} - {}",
                chrono::Local::now().format("%Y-%m-%d %H:%M:%S%.3f"),
                record.level(),
                record.target(),
                message
            ))
        });
    
    if log_file.is_some() {
        let file_config = log_format.chain(fern::log_file(PathBuf::from(log_file.as_ref().unwrap()))?);
        base_config.chain(file_config).apply()?;
    } else {
        let stdout_config = log_format.chain(io::stdout());
        base_config.chain(stdout_config).apply()?;
    }
    Ok(())
}