use log::{debug, error, info};
use notify::{RecommendedWatcher, RecursiveMode, Watcher};
use std::path::PathBuf;
use std::sync::mpsc::{channel, Receiver};
use std::time::Duration;
use structopt::StructOpt;

mod infra {
    pub mod logs;
    pub mod redis_client;
}

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
    infra::logs::setup_logs(cli_arguments.debug);
    debug!("Parsed CLI arguments: {:?}", cli_arguments);

    let client = infra::redis_client::RedisClient::new(cli_arguments.redis_url)?;

    if let Err(e) = watch(
        cli_arguments.paths_to_watch,
        cli_arguments.event_bounce_ms,
        client,
    ) {
        panic!("FATAL ERROR when watching: {:?}", e)
    }

    info!("terminating");
    Ok(())
}

fn watch(
    paths_to_watch: Vec<PathBuf>,
    event_bounce_ms: u64,
    client: infra::redis_client::RedisClient,
) -> notify::Result<Receiver<notify::DebouncedEvent>> {
    let (tx, event_channel) = channel();
    let mut watcher: RecommendedWatcher = Watcher::new(tx, Duration::from_millis(event_bounce_ms))?;

    for path in paths_to_watch {
        debug!("watching {:?}", path);
        (watcher.watch(path, RecursiveMode::Recursive))?;
    }

    loop {
        match event_channel.recv() {
            Ok(event) => handle_event(
                event,
                client
                    .get_connection()
                    .expect("connection to redis should be OK"),
            ),
            Err(e) => panic!("FATAL ERROR with the channel: {:?}", e),
        }
    }
}

fn handle_event(event: notify::DebouncedEvent, _connection: redis::Connection) {
    use notify::DebouncedEvent::*;

    debug!("got {:?}", event);

    match event {
        Create(_path) => (),
        Write(_path) => (),
        Remove(_path) => (),
        Rename(_old_path, _new_path) => (),
        NoticeWrite(_path) => (),  // do nothing
        NoticeRemove(_path) => (), // do nothing
        Chmod(_) => (),            // do nothing
        Rescan => debug!("rescanning watched paths"),
        Error(error, path) => error!("{} on path {:?}", error, path),
    }
}
