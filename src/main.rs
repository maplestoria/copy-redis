use std::io;
use std::path::PathBuf;

use structopt::clap::AppSettings;
use structopt::StructOpt;

#[derive(StructOpt, Debug)]
#[structopt(
no_version,
global_settings = & [AppSettings::DisableVersion]
)]
struct Opt {
    #[structopt(short)]
    source: String,
    
    #[structopt(short)]
    target: String,
    
    #[structopt(long)]
    discard_rdb: bool,
    
    #[structopt(long)]
    aof: bool,
    
    #[structopt(short = "l", parse(from_os_str))]
    logging_file: Option<PathBuf>,
}

fn main() {
    let opt: Opt = Opt::from_args();
    // 设置日志输出
    setup_logger(opt.logging_file).expect("logger设置失败");
}

fn setup_logger(log_file: Option<PathBuf>) -> Result<(), fern::InitError> {
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
            .chain(fern::log_file(log_file.unwrap().as_path())?);
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