extern crate getopts;

use std::env;
use std::io;
use std::net::ToSocketAddrs;
use std::path::PathBuf;
use std::process::exit;

use getopts::Options;
use redis::{ConnectionAddr, IntoConnectionInfo};
use redis_event::listener::standalone;
use redis_event::RedisListener;

fn main() {
    let args: Vec<String> = env::args().collect();
    let opt: Opt = parse_args(args);
    setup_logger(&opt.log_file).expect("logger设置失败");
    run(opt);
}

fn run(opt: Opt) {
    let source = opt.source.into_connection_info().expect("源Redis URI无效");
    let target = opt.target.into_connection_info().expect("目的Redis URI无效");
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
    
    let config = redis_event::config::Config {
        is_discard_rdb: opt.discard_rdb,
        is_aof: opt.aof,
        addr: socket_addr,
        password: source.passwd.unwrap_or_default(),
        repl_id: "?".to_string(),
        repl_offset: -1,
    };
    
    let mut listener = standalone::new(config);
    if let Err(error) = listener.open() {
        panic!("连接到源Redis错误: {}", error.to_string());
    }
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
    
    if log_file.is_some() {
        let file_config = fern::Dispatch::new()
            .format(|out, message, record| {
                out.finish(format_args!(
                    "{}[{}][{}] {}",
                    chrono::Local::now().format("[%Y-%m-%d][%H:%M:%S%.3f]"),
                    record.target(),
                    record.level(),
                    message
                ))
            })
            .chain(fern::log_file(PathBuf::from(log_file.as_ref().unwrap()))?);
        base_config
            .chain(file_config)
            .apply()?;
    } else {
        let stdout_config = fern::Dispatch::new()
            .format(|out, message, record| {
                out.finish(format_args!(
                    "{}[{}][{}] {}",
                    chrono::Local::now().format("[%Y-%m-%d][%H:%M:%S%.3f]"),
                    record.target(),
                    record.level(),
                    message
                ))
            })
            .chain(io::stdout());
        base_config
            .chain(stdout_config)
            .apply()?;
    }
    Ok(())
}