use anyhow::Context;
use log::debug;
use std::collections::hash_map::DefaultHasher;
use std::hash::Hasher;
use std::path::Path;

pub struct LocalFSStore;

impl LocalFSStore {
    pub fn remove_file(path: &Path) -> Result<(), anyhow::Error> {
        debug!("[local_fs_store] removing file {}", &path.display());
        std::fs::remove_file(&path)
            .with_context(|| format!("unable to remove file {}", &path.display()))
    }

    pub fn rename_file(old: &Path, new: &Path) -> Result<(), anyhow::Error> {
        debug!(
            "[local_fs_store] renaming file from {} to {}",
            &old.display(),
            &new.display()
        );

        LocalFSStore::ensure_directory_exists(&new)?;
        std::fs::rename(&old, &new).with_context(|| {
            format!(
                "unable to rename file from {} to {}",
                &old.display(),
                &new.display()
            )
        })
    }

    pub fn write_file(path: &Path, contents: Vec<u8>) -> Result<(), anyhow::Error> {
        debug!("[local_fs_store] writing file {}", &path.display());

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

    pub fn local_hash(path: &Path) -> Result<u64, anyhow::Error> {
        let mut hasher = DefaultHasher::default();
        let contents = std::fs::read(&path).context("unable to read file for hashing")?;
        hasher.write(&*contents);
        Ok(hasher.finish())
    }
}
