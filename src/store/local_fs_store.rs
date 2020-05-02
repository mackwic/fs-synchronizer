use anyhow::Context;
use log::debug;

pub struct LocalFSStore;

impl LocalFSStore {
    pub fn remove_file(path: String) -> Result<(), anyhow::Error> {
        debug!("[local_fs_store] removing file {}", &path);
        std::fs::remove_file(&path).with_context(|| format!("unable to remove file {}", &path))
    }

    pub fn rename_file(old: String, new: String) -> Result<(), anyhow::Error> {
        debug!("[local_fs_store] renaming file from {} to {}", &old, &new);
        // FIXME: make sure the parent directory exists
        std::fs::rename(&old, &new)
            .with_context(|| format!("unable to rename file from {} to {}", &old, &new))
    }

    pub fn write_file(path: String, contents: Vec<u8>) -> Result<(), anyhow::Error> {
        debug!("[local_fs_store] writing file {}", &path);

        // FIXME: make sure the parent directory exists
        std::fs::write(&path, contents)
            .with_context(|| format!("unable to write on local fs the file {}", &path))
    }
}
