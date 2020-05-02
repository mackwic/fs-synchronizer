use crate::client::redis_client::RedisPublishPayload;
use anyhow::bail;
use std::path::PathBuf;

pub enum FileEvents {
    New(PathBuf),
    Modified(PathBuf),
    Removed(PathBuf),
    Renamed(PathBuf, PathBuf),
}

pub static FILE_NEW: &str = "files:new";
pub static FILE_MODIFIED: &str = "files:modified";
pub static FILE_RENAMED: &str = "files:renamed";
pub static FILE_REMOVED: &str = "files:removed";

impl FileEvents {
    pub fn kind_as_str(&self) -> &str {
        match self {
            FileEvents::New(_) => FILE_NEW,
            FileEvents::Modified(_) => FILE_MODIFIED,
            FileEvents::Removed(_) => FILE_REMOVED,
            FileEvents::Renamed(_, _) => FILE_RENAMED,
        }
    }

    pub fn from_str_and_payload(
        kind: &str,
        payload: RedisPublishPayload,
    ) -> Result<FileEvents, anyhow::Error> {
        use RedisPublishPayload::*;

        let event = match (kind, payload) {
            ("files:new", OnePathMessage(_, path)) => FileEvents::New(path),
            ("files:modified", OnePathMessage(_, path)) => FileEvents::Modified(path),
            ("files:removed", OnePathMessage(_, path)) => FileEvents::Removed(path),
            ("files:renamed", TwoPathMessage(_, old, new)) => FileEvents::Renamed(old, new),
            (invalid_kind, invalid_payload) => bail!(
                "file event kind/payload has an invalid combination: {}/{:?}",
                invalid_kind,
                invalid_payload
            ),
        };
        Ok(event)
    }
}
