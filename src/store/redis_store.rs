use crate::client::redis_client::{RedisClient, RedisPublishPayload};
use crate::event_handler::file_events;
use anyhow::{bail, Context, Result};
use std::fs::File;
use std::io::Read;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone)]
pub struct RedisStore {
    client: RedisClient,
}

const SET_OF_ALL_FILES_NAME: &str = "all_files";

impl RedisStore {
    pub fn new(client: RedisClient) -> RedisStore {
        RedisStore { client }
    }

    pub fn new_file(&self, emitter_id: u64, path: PathBuf) -> Result<()> {
        let content = self
            .get_local_file_content(path.clone())
            .context("while looking for new file content")?;
        let publish_value = RedisPublishPayload::OnePathMessage(emitter_id, path.clone());
        let path_as_str = match path.to_str() {
            None => bail!(
                "path is not valid UTF-8 string. Unable to synchronize this file. Path: {:?}",
                &path.display()
            ),
            Some(path_as_str) => path_as_str,
        };
        self.client
            .in_transaction(|| {
                self.client.set(path_as_str, &content)?;
                self.client.sadd(SET_OF_ALL_FILES_NAME, path_as_str)?;
                self.client.publish(file_events::FILE_NEW, publish_value)
            })
            .context("unable to send redis commands to set new file")
    }

    pub fn modified_file(&self, emitter_id: u64, path: PathBuf) -> Result<()> {
        let content = self.get_local_file_content(path.clone())?;
        let publish_value = RedisPublishPayload::OnePathMessage(emitter_id, path.clone());
        let path_as_str = match path.to_str() {
            None => bail!(
                "path is not valid UTF-8 string. Unable to synchronize this file. Path: {:?}",
                &path.display()
            ),
            Some(path_as_str) => path_as_str,
        };

        self.client
            .in_transaction(|| {
                self.client.set(path_as_str, &content)?;
                self.client
                    .publish(file_events::FILE_MODIFIED, publish_value)
            })
            .context("unable to send the redis commands to modify the file")
    }

    pub fn renamed_file(
        &self,
        emitter_id: u64,
        old_path: PathBuf,
        new_path: PathBuf,
    ) -> Result<()> {
        let publish_value =
            RedisPublishPayload::TwoPathMessage(emitter_id, old_path.clone(), new_path.clone());
        let (old_path_as_str, new_path_as_str)  = match (old_path.to_str(), new_path.to_str()) {
            (Some(old), Some(new)) => (old, new),
            _ => bail!(
                "path is not valid UTF-8 string. Unable to synchronize this file. Old Path: {:?} New Path: {:?}",
                &old_path.display(), &new_path.display()
            ),
        };

        self.client
            .in_transaction(|| {
                self.client.rename(old_path_as_str, new_path_as_str)?;
                self.client
                    .smove(SET_OF_ALL_FILES_NAME, old_path_as_str, new_path_as_str)?;
                self.client
                    .publish(file_events::FILE_RENAMED, publish_value)
            })
            .context("unable to sned the redis commands to rename file")
    }

    pub fn removed_file(&self, emitter_id: u64, path: PathBuf) -> Result<()> {
        let publish_value = RedisPublishPayload::OnePathMessage(emitter_id, path.clone());
        let path_as_str = match path.to_str() {
            None => bail!(
                "path is not valid UTF-8 string. Unable to synchronize this file. Path: {:?}",
                &path.display()
            ),
            Some(path_as_str) => path_as_str,
        };
        self.client
            .in_transaction(|| {
                self.client.remove(path_as_str)?;
                self.client.srem(SET_OF_ALL_FILES_NAME, path_as_str)?;
                self.client
                    .publish(file_events::FILE_REMOVED, publish_value)
            })
            .context("unable to send the redis commands to remove file")
    }

    pub fn get_all_remote_files(&self) -> Result<Vec<String>> {
        self.client
            .smembers(SET_OF_ALL_FILES_NAME)
            .context("unable to send the redis command to list all the files")
    }

    pub fn get_remote_file_content(&self, path: &Path) -> Result<Vec<u8>> {
        self.client.get(&path.to_string_lossy())
    }

    pub fn get_local_file_content(&self, path: PathBuf) -> Result<Vec<u8>> {
        let mut contents: Vec<u8> = Vec::with_capacity(8196);
        let mut file = File::open(path.clone())
            .with_context(|| format!("unable to open file {}", path.clone().display()))?;

        file.read_to_end(&mut contents)
            .with_context(|| format!("unable to read file {}", path.clone().display()))?;
        Ok(contents)
    }
}
