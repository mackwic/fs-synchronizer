use anyhow::Context;
use log::debug;
use std::path::{Path, PathBuf};

pub struct LocalFSStore;

impl LocalFSStore {
    pub fn remove_file(path: String) -> Result<(), anyhow::Error> {
        debug!("[local_fs_store] removing file {}", &path);
        std::fs::remove_file(&path).with_context(|| format!("unable to remove file {}", &path))
    }

    pub fn rename_file(old: String, new: String) -> Result<(), anyhow::Error> {
        debug!("[local_fs_store] renaming file from {} to {}", &old, &new);
        let new_path = PathBuf::from(new);

        LocalFSStore::ensure_directory_exists(&new_path)?;
        std::fs::rename(&old, &new_path).with_context(|| {
            format!(
                "unable to rename file from {} to {}",
                &old,
                &new_path.display()
            )
        })
    }

    pub fn write_file(path: String, contents: Vec<u8>) -> Result<(), anyhow::Error> {
        debug!("[local_fs_store] writing file {}", &path);
        let path = PathBuf::from(path);

        LocalFSStore::ensure_directory_exists(&path)?;
        std::fs::write(&path, contents)
            .with_context(|| format!("unable to write on local fs the file {}", &path.display()))
    }

    pub fn ensure_directory_exists(path: &Path) -> Result<(), anyhow::Error> {
        let parent_directory: &Path = path.parent().context("new file cannot be /")?;
        if parent_directory.exists() {
            Ok(())
        } else {
            std::fs::create_dir_all(parent_directory).with_context(|| {
                format!(
                    "unable to create directories holding new path {}",
                    &path.display()
                )
            })
        }
    }
}
