use crate::client::redis_client::{RedisClient, RedisPublishPayload};
use crate::event_handler::file_events;
use anyhow::{bail, Context, Result};
use std::collections::hash_map::DefaultHasher;
use std::fs::File;
use std::hash::Hasher;
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
        let hash = self.hash_content(&*content);

        let publish_value = RedisPublishPayload::NewFile(emitter_id, hash, path.clone());
        let path_as_str = match path.to_str() {
            None => bail!(
                "path is not valid UTF-8 string. Unable to synchronize this file. Path: {:?}",
                &path.display()
            ),
            Some(path_as_str) => path_as_str,
        };
        self.client
            .in_transaction(|| {
                self.client
                    .set(&self.to_hash_key(path_as_str), hash.to_string().as_bytes())?;
                self.client
                    .set(&self.to_content_key(path_as_str), &content)?;
                self.client.sadd(SET_OF_ALL_FILES_NAME, path_as_str)?;
                self.client.publish(file_events::FILE_EVENT, publish_value)
            })
            .context("unable to send redis commands to set new file")
    }

    pub fn modified_file(&self, emitter_id: u64, path: PathBuf) -> Result<()> {
        let content = self
            .get_local_file_content(path.clone())
            .context("while looking for modified file content")?;
        let hash = self.hash_content(&*content);
        let publish_value = RedisPublishPayload::ModifiedFile(emitter_id, hash, path.clone());
        let path_as_str = match path.to_str() {
            None => bail!(
                "path is not valid UTF-8 string. Unable to synchronize this file. Path: {:?}",
                &path.display()
            ),
            Some(path_as_str) => path_as_str,
        };

        self.client
            .in_transaction(|| {
                self.client
                    .set(&self.to_hash_key(path_as_str), hash.to_string().as_bytes())?;
                self.client
                    .set(&self.to_content_key(path_as_str), &content)?;
                self.client.publish(file_events::FILE_EVENT, publish_value)
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
            RedisPublishPayload::RenamedFile(emitter_id, old_path.clone(), new_path.clone());
        let (old_path_as_str, new_path_as_str)  = match (old_path.to_str(), new_path.to_str()) {
            (Some(old), Some(new)) => (old, new),
            _ => bail!(
                "path is not valid UTF-8 string. Unable to synchronize this file. Old Path: {:?} New Path: {:?}",
                &old_path.display(), &new_path.display()
            ),
        };

        self.client
            .in_transaction(|| {
                self.client.rename(
                    &self.to_hash_key(old_path_as_str),
                    &self.to_hash_key(new_path_as_str),
                )?;
                self.client.rename(
                    &self.to_content_key(old_path_as_str),
                    &self.to_content_key(new_path_as_str),
                )?;
                self.client
                    .smove(SET_OF_ALL_FILES_NAME, old_path_as_str, new_path_as_str)?;
                self.client.publish(file_events::FILE_EVENT, publish_value)
            })
            .context("unable to sned the redis commands to rename file")
    }

    pub fn removed_file(&self, emitter_id: u64, path: PathBuf) -> Result<()> {
        let publish_value = RedisPublishPayload::RemovedFile(emitter_id, path.clone());
        let path_as_str = match path.to_str() {
            None => bail!(
                "path is not valid UTF-8 string. Unable to synchronize this file. Path: {:?}",
                &path.display()
            ),
            Some(path_as_str) => path_as_str,
        };
        self.client
            .in_transaction(|| {
                self.client.remove(&self.to_hash_key(path_as_str))?;
                self.client.remove(&self.to_content_key(path_as_str))?;
                self.client.srem(SET_OF_ALL_FILES_NAME, path_as_str)?;
                self.client.publish(file_events::FILE_EVENT, publish_value)
            })
            .context("unable to send the redis commands to remove file")
    }

    pub fn get_all_remote_files(&self) -> Result<Vec<String>> {
        self.client
            .smembers(SET_OF_ALL_FILES_NAME)
            .context("unable to send the redis command to list all the files")
    }

    pub fn get_remote_file_content(&self, path: &Path) -> Result<Vec<u8>> {
        let mut contents: Vec<u8> = Vec::with_capacity(8196);
        {
            let compressed_content = self
                .client
                .get(&self.to_content_key(&path.to_string_lossy()))
                .context("unable to read compressed file content from redis server")?;
            let mut decompressing_writer = snap::read::FrameDecoder::new(&*compressed_content);
            std::io::copy(&mut decompressing_writer, &mut contents)
                .context("error when decoding compressed content")?;
        }
        Ok(contents)
    }

    pub fn get_remote_file_hash(&self, path: &Path) -> Result<u64> {
        let raw_num = self
            .client
            .get(&self.to_hash_key(&path.to_string_lossy()))
            .with_context(|| {
                format!(
                    "unable to get on redis server the hash of file {}",
                    &path.display()
                )
            })?;
        let str_num = String::from_utf8_lossy(&raw_num);
        let hash: u64 = str_num
            .parse()
            .context("unable to parse redis value to a correct hash")?;
        Ok(hash)
    }

    pub fn get_local_file_content(&self, path: PathBuf) -> Result<Vec<u8>> {
        let mut contents: Vec<u8> = Vec::with_capacity(8196);
        {
            let mut compressing_writer = snap::write::FrameEncoder::new(&mut contents);
            let mut file = File::open(path.clone())
                .with_context(|| format!("unable to open file {}", path.clone().display()))?;

            std::io::copy(&mut file, &mut compressing_writer)
                .with_context(|| format!("unable to read file {}", path.clone().display()))?;
        }
        Ok(contents)
    }

    fn to_hash_key(&self, path: &str) -> String {
        format!("hash:{}", path)
    }

    fn to_content_key(&self, path: &str) -> String {
        format!("content:{}", path)
    }

    fn hash_content(&self, content: &[u8]) -> u64 {
        let mut hasher = DefaultHasher::default();
        hasher.write(&*content);
        hasher.finish()
    }
}
