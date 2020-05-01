use log::{debug, error};
use notify::{RecommendedWatcher, RecursiveMode, Watcher};
use std::path::PathBuf;
use std::sync::mpsc::{channel, Receiver};
use std::time::Duration;
use structopt::StructOpt;

mod logs;

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

fn main() {
    let cli_arguments = Opt::from_args();
    logs::setup_logs(cli_arguments.debug);
    debug!("Parsed CLI arguments: {:?}", cli_arguments);

    let client = match test_redis_connection(cli_arguments.redis_url) {
        Err(e) => panic!(
            "FATAL ERROR: unable to connect to redis store: {}",
            e.as_ref()
        ),
        Ok(client) => client,
    };

    if let Err(e) = watch(
        cli_arguments.paths_to_watch,
        cli_arguments.event_bounce_ms,
        client,
    ) {
        panic!("FATAL ERROR when watching: {:?}", e)
    }
}

fn test_redis_connection(redis_url: String) -> Result<redis::Client, Box<dyn std::error::Error>> {
    let client = redis::Client::open(redis_url)?;
    let mut connection = client.get_connection_with_timeout(Duration::from_secs(1))?;
    let response = redis::cmd("PING").query::<String>(&mut connection)?;
    if response == "PONG" {
        debug!("redis connection OK");
        Ok(client)
    } else {
        panic!(
            "FATAL ERROR when pinging redis, I expected PONG but got {}",
            response,
        )
    }
}

fn watch(
    paths_to_watch: Vec<PathBuf>,
    event_bounce_ms: u64,
    client: redis::Client,
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
