use crate::client::redis_client::RedisClient;
use crate::event_handler::file_events::{self, FileEvents};
use crate::store::redis_store::RedisStore;
use anyhow::Context;
use log::{debug, error};
use std::thread::JoinHandle;

pub struct RemoteFileEventHandler {
    client: RedisClient,
    store: RedisStore,
    unique_id: u64,
}

impl RemoteFileEventHandler {
    pub fn new(client: RedisClient, store: RedisStore, unique_id: u64) -> RemoteFileEventHandler {
        RemoteFileEventHandler {
            client,
            store,
            unique_id,
        }
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
            let event_kind = msg.get_channel_name();
            let payload: String = msg.get_payload()?;
            let emitter_id: u64 = payload[0..20].parse()?;
            let path: &str = &payload[21..];
            debug!(
                "[remote_file] got message on channel '{}' from emitter {}: {}",
                event_kind, emitter_id, path
            );

            if emitter_id == self.unique_id {
                debug!("[remote_file] skipping event as we are the emitter")
            } else {
                self.handle_event(event_kind, path)
            }
        }
    }

    fn handle_event(&self, event_kind: &str, path: &str) {
        debug!("[remote_file] got {} with {}", event_kind, path);

        let event = match file_events::FileEvents::from_str(event_kind, &[path]) {
            Ok(event) => event,
            Err(error) => {
                error!("Error when handling event: {:?}", error);
                return;
            }
        };

        let res = match event {
            FileEvents::New(path) => self.write_file(path),
            FileEvents::Modified(path) => self.write_file(path),
            FileEvents::Removed(path) => self.remove_file(path),
            FileEvents::Renamed(old, new) => self.rename_file(old, new),
        };

        if let Err(error) = res {
            error!("Error when applying event to local fs: {:?}", error);
        }
    }

    fn remove_file(&self, path: String) -> Result<(), anyhow::Error> {
        debug!("[remote_file] removing file {}", &path);
        std::fs::remove_file(&path).with_context(|| format!("unable to remove file {}", &path))
    }

    fn rename_file(&self, old: String, new: String) -> Result<(), anyhow::Error> {
        debug!("[remote_file] renaming file from {} to {}", &old, &new);
        std::fs::rename(&old, &new)
            .with_context(|| format!("unable to rename file from {} to {}", &old, &new))
    }

    fn write_file(&self, path: String) -> Result<(), anyhow::Error> {
        debug!("[remote_file] writing file {}", &path);
        let contents = self
            .store
            .get_remote_file_content(&path)
            .with_context(|| format!("unable to get from redis file content of {}", &path))?;
        std::fs::write(&path, contents)
            .with_context(|| format!("unable to write on local fs the file {}", &path))
    }
}
