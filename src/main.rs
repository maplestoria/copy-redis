extern crate ctrlc;
extern crate getopts;

use std::cell::RefCell;
use std::collections::hash_map::DefaultHasher;
use std::env;
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

fn main() {
    let args: Vec<String> = env::args().collect();
    let opt: Opt = parse_args(args);
    setup_logger(&opt.log_file).expect("logger设置失败");
    run(opt);
}

fn run(opt: Opt) {
    let source = opt.source.into_connection_info().expect("源Redis URI无效");
    let target = opt.target.clone().into_connection_info().expect("目的Redis URI无效");
    if source.addr == target.addr {
        panic!("Error: 源Redis地址不能与目的Redis地址相同");
    }
    
    let socket_addr;
    if let ConnectionAddr::Tcp(host, port) = source.addr.as_ref() {
        let addr = format!("{}:{}", host, port);
        let mut iter = addr.to_socket_addrs().expect("");
        socket_addr = iter.next().unwrap();
    } else {
        unimplemented!("Unix Domain Socket");
    }
    let source_addr = socket_addr.to_string();
    
    let mut config = redis_event::config::Config {
        is_discard_rdb: opt.discard_rdb,
        is_aof: opt.aof,
        addr: socket_addr,
        password: source.passwd.unwrap_or_default(),
        repl_id: "?".to_string(),
        repl_offset: -1,
        read_timeout: Option::Some(Duration::from_millis(2000)),
        write_timeout: Option::Some(Duration::from_millis(2000)),
    };
    
    if let Ok((repl_id, repl_offset)) = load_repl_meta(&source_addr) {
        info!("获取到REPL历史记录信息, id: {}, offset: {}", repl_id, repl_offset);
        config.repl_id = repl_id;
        config.repl_offset = repl_offset;
    }
    
    // 先关闭listener，因为listener在读取流中的数据时，是阻塞的，
    // 所以在接收到ctrl-c信号的时候，得再等一会，等redis master的数据来到(或者读取超时)，此时，程序才会继续运行，
    // 等命令被handler处理完之后，listener才能结束，而且handler的结束还必须在listener之后，要不然丢数据
    let listener_running = Arc::new(AtomicBool::new(true));
    let r1 = listener_running.clone();
    ctrlc::set_handler(move || {
        r1.store(false, Ordering::SeqCst);
    }).expect("Error setting Ctrl-C handler");
    
    let mut listener = standalone::new(config, listener_running);
    
    let event_handler = handler::new(&opt.target);
    listener.set_event_handler(Rc::new(RefCell::new(event_handler)));
    
    if let Err(error) = listener.open() {
        error!("连接到源Redis错误: {}", error.to_string());
    }
    // 程序正常退出时，保存repl id和offset
    if let Err(err) = save_repl_meta(&source_addr, &listener.config.repl_id, listener.config.repl_offset) {
        error!("保存REPL信息失败:{}", err);
    }
}

fn load_repl_meta(source_addr: &str) -> io::Result<(String, i64)> {
    let mut s = DefaultHasher::new();
    source_addr.hash(&mut s);
    let hash = s.finish();
    let path = format!(".copy_redis/{}", hash);
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
    Err(Error::new(io::ErrorKind::InvalidData, "未能获取到有效的REPL历史信息"))
}

fn save_repl_meta(source_addr: &str, id: &str, offset: i64) -> io::Result<()> {
    let mut s = DefaultHasher::new();
    source_addr.hash(&mut s);
    let hash = s.finish();
    let path = format!(".copy_redis/{}", hash);
    if let Err(_) = fs::metadata(".copy_redis") {
        fs::create_dir(".copy_redis")?;
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
    target: String,
    discard_rdb: bool,
    aof: bool,
    log_file: Option<String>,
}

fn parse_args(args: Vec<String>) -> Opt {
    let mut opts = Options::new();
    opts.optopt("s", "source", "此Redis内的数据将复制到目的Redis中. URI格式形如: \"redis://[:password@]host:port\", 中括号及其内容可省略", "源Redis的URI");
    opts.optopt("t", "target", "URI格式同上", "目的Redis的URI");
    opts.optflag("", "discard-rdb", "是否跳过整个RDB不进行复制, 默认为false, 复制完整的RDB");
    opts.optflag("", "aof", "是否需要处理AOF, 默认为false, 当RDB复制完后, 程序将终止");
    opts.optopt("l", "log", "日志输出文件. 不指定此选项, 日志将输出至标准输出流", "");
    opts.optflag("h", "help", "输出帮助信息");
    
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
    
    let (source, target) = if matches.opt_present("s") && matches.opt_present("t") {
        (matches.opt_str("s").unwrap(), matches.opt_str("t").unwrap())
    } else {
        eprint!("Error: {}\r\n\r\n", "请同时指定source与target参数");
        print_usage(&opts);
        exit(1);
    };
    
    let discard_rdb = matches.opt_present("discard-rdb");
    let aof = matches.opt_present("aof");
    let log_file = matches.opt_str("l");
    
    return Opt {
        source,
        target,
        discard_rdb,
        aof,
        log_file,
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
                "{}[{}][{}] {}",
                chrono::Local::now().format("[%Y-%m-%d][%H:%M:%S%.3f]"),
                record.target(),
                record.level(),
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