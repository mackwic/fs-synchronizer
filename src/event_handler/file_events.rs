use anyhow::bail;

pub enum FileEvents {
    New(String),
    Modified(String),
    Removed(String),
    Renamed(String, String),
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

    pub fn from_str(kind: &str, value: &[&str]) -> Result<FileEvents, anyhow::Error> {
        let event = match kind {
            "files:new" => FileEvents::New(String::from(value[0])),
            "files:modified" => FileEvents::Modified(String::from(value[0])),
            "files:removed" => FileEvents::Removed(String::from(value[0])),
            "files:renamed" => FileEvents::Renamed(String::from(value[0]), String::from(value[1])),
            invalid_kind => bail!("file event kind is invalid: {}", invalid_kind),
        };
        Ok(event)
    }
}
