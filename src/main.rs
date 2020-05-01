use log::{debug, info};
use notify::{RecommendedWatcher, RecursiveMode, Watcher};
use std::path::PathBuf;
use std::sync::mpsc::{channel, Receiver};
use std::time::Duration;
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
    debug!("Parsed CLI arguments: {:?}", cli_arguments);

    let client = client::redis_client::RedisClient::new(cli_arguments.redis_url)?;
    let store = store::redis_store::RedisStore::new(client);
    let event_handler = event_handler::local_file::LocalFileEventHandler::new(store);

    if let Err(e) = watch(
        cli_arguments.paths_to_watch,
        cli_arguments.event_bounce_ms,
        event_handler,
    ) {
        panic!("FATAL ERROR when watching: {:?}", e)
    }

    info!("terminating");
    Ok(())
}

fn watch(
    paths_to_watch: Vec<PathBuf>,
    event_bounce_ms: u64,
    handler: event_handler::local_file::LocalFileEventHandler,
) -> notify::Result<Receiver<notify::DebouncedEvent>> {
    let (tx, event_channel) = channel();
    let mut watcher: RecommendedWatcher = Watcher::new(tx, Duration::from_millis(event_bounce_ms))?;

    for path in paths_to_watch {
        debug!("watching {:?}", path);
        (watcher.watch(path, RecursiveMode::Recursive))?;
    }

    loop {
        match event_channel.recv() {
            Ok(event) => handler.handle_event(event),
            Err(e) => panic!("FATAL ERROR with the channel: {:?}", e),
        }
    }
}
