use crate::client::redis_client::RedisClient;
use anyhow::Context;
use log::{debug, error};
use std::thread::JoinHandle;

pub struct RemoteFileEventHandler {
    client: RedisClient,
}

impl RemoteFileEventHandler {
    pub fn new(client: RedisClient) -> RemoteFileEventHandler {
        RemoteFileEventHandler { client }
    }

    pub fn watch_events(self) -> Result<JoinHandle<()>, anyhow::Error> {
        let handle = std::thread::Builder::new()
            .name(String::from("remote file events thread"))
            .spawn(move || {
                if let Err(error) = self.start_watching() {
                    panic!("Error in thread: {}", error)
                }
            })
            .context("unable to create remote file events thread")?;
        Ok(handle)
    }

    pub fn handle_event(&self, event: notify::DebouncedEvent) {
        use notify::DebouncedEvent::*;

        debug!("[remote_file] got {:?}", event);

        match event {
            Create(_path) => (),
            Write(_path) => (),
            Remove(_path) => (),
            Rename(_old_path, _new_path) => (),
            NoticeWrite(_path) => (),  // do nothing
            NoticeRemove(_path) => (), // do nothing
            Chmod(_) => (),            // do nothing
            Rescan => debug!("[remote_file] rescanning watched paths"),
            Error(error, path) => error!("{} on path {:?}", error, path),
        }
    }

    fn start_watching(&self) -> Result<(), anyhow::Error> {
        debug!("[remote_file] subscribing to redis...");
        let mut connection = self
            .client
            .take_connection()
            .context("unable to take connection to Redis server")?;
        let mut pubsub: r2d2_redis::redis::PubSub = connection.as_pubsub();
        pubsub
            .psubscribe("files:*")
            .context("unable to subscribe to redis channels `files:*`")?;

        loop {
            let msg = pubsub.get_message()?;
            let payload: String = msg.get_payload()?;
            debug!(
                "[remote_file] got message on channel '{}': {}",
                msg.get_channel_name(),
                payload
            );
        }
    }
}
