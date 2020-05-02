use crate::client::redis_client::{RedisClient, RedisPublishPayload};
use crate::event_handler::file_events::{self, FileEvents};
use crate::store::local_fs_store::LocalFSStore;
use crate::store::redis_store::RedisStore;
use anyhow::Context;
use log::{debug, error};
use std::path::PathBuf;
use std::thread::JoinHandle;

pub struct RemoteFilesEventHandler {
    client: RedisClient,
    store: RedisStore,
    unique_id: u64,
}

impl RemoteFilesEventHandler {
    pub fn new(client: RedisClient, store: RedisStore, unique_id: u64) -> RemoteFilesEventHandler {
        RemoteFilesEventHandler {
            client,
            store,
            unique_id,
        }
    }

    pub fn synchronize_local_files_with_remote(&self) -> Result<(), anyhow::Error> {
        debug!("[remote_file] synchronizing all remote files to local fs");

        let remote_files = self
            .store
            .get_all_remote_files()
            .context("when synchronizing local files with remote files")?;

        for path in remote_files {
            debug!("[remote_file] retreiving {}...", path);
            let path = PathBuf::from(path);
            let contents = match self.store.get_remote_file_content(&path) {
                Err(error) => {
                    error!(
                        "unable to retreive file {} from remote storage. Error: {:?}",
                        &path.display(),
                        error
                    );
                    continue;
                }
                Ok(content) => content,
            };

            if let Err(error) = LocalFSStore::write_file(&path, contents) {
                error!(
                    "unable to write file {} on local storage ! Error: {:?}",
                    &path.display(),
                    error
                );
                continue;
            }
        }

        debug!("[remote_file] synchronization complete");
        Ok(())
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

            let payload_res: Result<RedisPublishPayload, rmp_serde::decode::Error> =
                rmp_serde::from_slice(msg.get_payload_bytes());

            let payload = match payload_res {
                Err(error) => {
                    debug!(
                        "error when decoding message. Skipping message. Detailed error: {:?}",
                        error
                    );
                    continue;
                }
                Ok(payload) => payload,
            };
            debug!(
                "[remote_file] got message on channel '{}': {:?}",
                event_kind, payload
            );

            if payload.get_emitter_id() == self.unique_id {
                debug!("[remote_file] skipping event as we are the emitter");
                continue;
            }
            let handling_result = self.handle_event(event_kind, payload);
            if let Err(error) = handling_result {
                error!("Error when handling event: {:?}", error)
            }
        }
    }

    fn handle_event(
        &self,
        event_kind: &str,
        payload: RedisPublishPayload,
    ) -> Result<(), anyhow::Error> {
        let event = file_events::FileEvents::from_str_and_payload(event_kind, payload)
            .context("unable to convert the event to a known file event")?;

        let res = match event {
            FileEvents::New(path) | FileEvents::Modified(path) => {
                let contents = self.store.get_remote_file_content(&path).with_context(|| {
                    format!(
                        "unable to get from redis file content of {}",
                        &path.display()
                    )
                })?;
                LocalFSStore::write_file(&path, contents)
            }
            FileEvents::Removed(path) => LocalFSStore::remove_file(&path),
            FileEvents::Renamed(old, new) => LocalFSStore::rename_file(&old, &new),
        };

        if res.is_err() {
            return res.context("Error when applying event to local fs");
        }
        Ok(())
    }
}
