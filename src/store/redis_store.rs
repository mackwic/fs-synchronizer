use crate::client::redis_client::RedisClient;
use crate::event_handler::file_events;
use anyhow::{bail, Context, Result};
use std::fs::File;
use std::io::Read;
use std::path::PathBuf;

#[derive(Debug, Clone)]
pub struct RedisStore {
    client: RedisClient,
}

impl RedisStore {
    pub fn new(client: RedisClient) -> RedisStore {
        RedisStore { client }
    }

    pub fn new_file(&self, emmiter_id: u64, path: PathBuf) -> Result<()> {
        let content = self
            .get_local_file_content(path.clone())
            .context("while looking for new file content")?;
        let path_as_str = match path.to_str() {
            None => bail!(
                "dropped file event because path is not a valid UTF-8 string. Path is {:?}",
                path
            ),
            Some(path) => path,
        };

        self.client
            .in_transaction(|| {
                self.client.set(path_as_str, &content)?;
                self.client.sadd("file_set", path_as_str)?;
                // left-pad the emitter id with 0 if needed so that the size is fixed
                let publish_value = format!("{:0>20}:{}", emmiter_id, path_as_str);
                self.client.publish(file_events::FILE_NEW, &publish_value)
            })
            .context("unable to send redis commands to set new file")
    }

    pub fn modified_file(&self, emmiter_id: u64, path: PathBuf) -> Result<()> {
        let content = self.get_local_file_content(path.clone())?;
        let path_as_str = match path.to_str() {
            None => bail!(
                "dropped file event because path is not a valid UTF-8 string. Path is {:?}",
                path
            ),
            Some(path) => path,
        };

        self.client
            .in_transaction(|| {
                self.client.set(path_as_str, &content)?;
                // left-pad the emitter id with 0 if needed so that the size is fixed
                let publish_value = format!("{:0>20}:{}", emmiter_id, path_as_str);
                self.client
                    .publish(file_events::FILE_MODIFIED, &publish_value)
            })
            .context("unable to send the redis commands to modify the file")
    }

    pub fn renamed_file(
        &self,
        emmiter_id: u64,
        old_path: PathBuf,
        new_path: PathBuf,
    ) -> Result<()> {
        let (old_path_as_str, new_path_as_str) = match (old_path.to_str(), new_path.to_str()) {
            (None, _) | (_, None) => bail!(
                "dropped file event because path is not a valid UTF-8 string. Path are {:?} and {:?}",
                old_path, new_path
            ),
            (Some(old_path), Some(new_path)) => (old_path, new_path),
        };

        self.client
            .in_transaction(|| {
                self.client.rename(old_path_as_str, new_path_as_str)?;
                self.client
                    .smove("file_set", old_path_as_str, new_path_as_str)?;
                // left-pad the emitter id with 0 if needed so that the size is fixed
                let publish_value = format!(
                    "{:0>20}:{}:{}",
                    emmiter_id, old_path_as_str, new_path_as_str
                );
                self.client
                    .publish(file_events::FILE_RENAMED, &publish_value)
            })
            .context("unable to sned the redis commands to rename file")
    }

    pub fn removed_file(&self, emmiter_id: u64, path: PathBuf) -> Result<()> {
        let path_as_str = match path.to_str() {
            None => bail!(
                "dropped file event because path is not a valid UTF-8 string. Path is {:?}",
                path
            ),
            Some(path) => path,
        };

        self.client
            .in_transaction(|| {
                self.client.remove(path_as_str)?;
                self.client.srem("file_set", path_as_str)?;
                // left-pad the emitter id with 0 if needed so that the size is fixed
                let publish_value = format!("{:0>20}:{}", emmiter_id, path_as_str);
                self.client
                    .publish(file_events::FILE_REMOVED, &publish_value)
            })
            .context("unable to send the redis commands to remove file")
    }

    pub fn get_remote_file_content(&self, path: &str) -> Result<Vec<u8>> {
        self.client.get(path)
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
