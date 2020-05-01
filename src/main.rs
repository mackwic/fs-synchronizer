use log::{debug, error, info};
use std::path::PathBuf;
use structopt::StructOpt;

pub mod client {
    pub mod redis_client;
}
pub mod event_handler {
    pub mod local_file;
    pub mod remote_file;
}
pub mod store {
    pub mod redis_store;
}
pub mod logs;

#[derive(Debug, StructOpt)]
#[structopt(name = "fs-on-redis", about = "Synchronize the FS on a Redis DB")]
struct Opt {
    /// Enable debug logs
    #[structopt(short, long)]
    debug: bool,

    /// Path to watch
    #[structopt(parse(from_os_str), default_value = ".", env)]
    paths_to_watch: Vec<PathBuf>,

    /// Event bouncing duration in milliseconds
    #[structopt(short, long, default_value = "100", env)]
    event_bounce_ms: u64,

    /// Connection string to redis
    #[structopt(long, env)]
    redis_url: String,
}

fn main() -> Result<(), anyhow::Error> {
    let cli_arguments = Opt::from_args();
    logs::setup_logs(cli_arguments.debug);
    debug!("[main] Parsed CLI arguments: {:?}", cli_arguments);

    let client = client::redis_client::RedisClient::new(cli_arguments.redis_url)?;
    let store = store::redis_store::RedisStore::new(client);
    let local_file_watcher = event_handler::local_file::LocalFileEventHandler::new(
        store,
        cli_arguments.paths_to_watch,
        cli_arguments.event_bounce_ms,
    );

    let thread_handle = local_file_watcher.watch_events()?;

    if thread_handle.join().is_err() {
        error!("Thread terminated in error");
    }
    info!("terminating");
    Ok(())
}
