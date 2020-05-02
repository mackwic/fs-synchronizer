use crate::client::redis_client::RedisPublishPayload;
use anyhow::bail;
use std::path::PathBuf;

pub enum FileEvents {
    /// (absolute path, hash)
    New(PathBuf, u64),
    /// (absolute path, hash)
    Modified(PathBuf, u64),
    /// (absolute path)
    Removed(PathBuf),
    /// (absolute path, hash)
    Renamed(PathBuf, PathBuf),
}

pub static FILE_EVENT: &str = "file_event";

impl FileEvents {
    pub fn kind_as_str(&self) -> &str {
        FILE_EVENT
    }

    pub fn from_str_and_payload(
        kind: &str,
        payload: RedisPublishPayload,
    ) -> Result<FileEvents, anyhow::Error> {
        use RedisPublishPayload::*;

        if kind != FILE_EVENT {
            bail!("unknown event kind: {}", kind,);
        }

        let event = match payload {
            NewFile(_, hash, path) => FileEvents::New(path, hash),
            ModifiedFile(_, hash, path) => FileEvents::Modified(path, hash),
            RemovedFile(_, path) => FileEvents::Removed(path),
            RenamedFile(_, old, new) => FileEvents::Renamed(old, new),
        };
        Ok(event)
    }
}
