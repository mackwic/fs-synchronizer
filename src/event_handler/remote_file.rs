use crate::client::redis_client::RedisClient;
use crate::event_handler::file_events::{self, FileEvents};
use crate::store::local_fs_store::LocalFSStore;
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
                debug!("[remote_file] skipping event as we are the emitter");
                continue;
            }
            let handling_result = self.handle_event(event_kind, path);
            if let Err(error) = handling_result {
                error!("Error when handling event: {:?}", error)
            }
        }
    }

    fn handle_event(&self, event_kind: &str, path: &str) -> Result<(), anyhow::Error> {
        debug!("[remote_file] got {} with {}", event_kind, path);

        let event = file_events::FileEvents::from_str(event_kind, &[path])
            .context("unable to convert the event to a known file event")?;

        let res = match event {
            FileEvents::New(path) | FileEvents::Modified(path) => {
                let contents = self.store.get_remote_file_content(&path).with_context(|| {
                    format!("unable to get from redis file content of {}", &path)
                })?;
                LocalFSStore::write_file(path, contents)
            }
            FileEvents::Removed(path) => LocalFSStore::remove_file(path),
            FileEvents::Renamed(old, new) => LocalFSStore::rename_file(old, new),
        };

        if res.is_err() {
            return res.context("Error when applying event to local fs");
        }
        Ok(())
    }
}
