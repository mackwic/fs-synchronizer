use crate::client::redis_client::RedisClient;
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

    pub fn new_file(&self, path: PathBuf) -> Result<()> {
        let content = self.get_file_content(path.clone())?;
        let path_as_str = match path.to_str() {
            None => bail!(
                "dropped file event because path is not a valid UTF-8 string. Path is {:?}",
                path
            ),
            Some(path) => path,
        };

        self.client.set(path_as_str, &content)?;
        self.client.publish("files:new", path_as_str)?;

        Ok(())
    }

    fn get_file_content(&self, path: PathBuf) -> Result<Vec<u8>> {
        let mut contents: Vec<u8> = Vec::with_capacity(8196);
        let mut file = File::open(path.clone())
            .with_context(|| format!("unable to open file {}", path.clone().display()))?;

        file.read_to_end(&mut contents)
            .with_context(|| format!("unable to read file {}", path.clone().display()))?;
        Ok(contents)
    }
}
