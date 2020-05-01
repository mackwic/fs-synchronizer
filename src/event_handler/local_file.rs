use crate::store::redis_store::RedisStore;
use anyhow::{anyhow, Context, Result};
use log::{debug, error};
use notify::{RecommendedWatcher, RecursiveMode, Watcher};
use std::path::PathBuf;
use std::sync::mpsc::channel;
use std::thread::JoinHandle;
use std::time::Duration;

pub struct LocalFileEventHandler {
    event_bounce_ms: u64,
    unique_id: u64,
    paths_to_watch: Vec<PathBuf>,
    store: RedisStore,
}

impl LocalFileEventHandler {
    pub fn new(
        store: RedisStore,
        unique_id: u64,
        paths_to_watch: Vec<PathBuf>,
        event_bounce_ms: u64,
    ) -> LocalFileEventHandler {
        LocalFileEventHandler {
            event_bounce_ms,
            unique_id,
            paths_to_watch,
            store,
        }
    }

    pub fn watch_events(self) -> Result<JoinHandle<()>, anyhow::Error> {
        let handle = std::thread::Builder::new()
            .name(String::from("local files watcher"))
            .spawn(move || {
                if let Err(error) = self.start_watching() {
                    panic!("Error in thread: {:?}", error);
                }
            })
            .context("local file thread creation")?;
        Ok(handle)
    }

    pub fn handle_event(&self, event: notify::DebouncedEvent) {
        use notify::DebouncedEvent::*;

        debug!("[local_file] got {:?}", event);

        let res = match event {
            Create(path) => self.store.new_file(self.unique_id, path),
            Write(path) => self.store.modified_file(self.unique_id, path),
            Remove(path) => self.store.removed_file(self.unique_id, path),
            Rename(old_path, new_path) => {
                self.store.renamed_file(self.unique_id, old_path, new_path)
            }
            NoticeWrite(_path) => Ok(()),  // do nothing
            NoticeRemove(_path) => Ok(()), // do nothing
            Chmod(_) => Ok(()),            // do nothing
            Rescan => {
                debug!("[local_file] rescanning watched paths");
                Ok(())
            }
            Error(error, path) => Err(anyhow!("Error: {} on path {:?}", error, path)),
        };

        if let Err(error) = res {
            error!("Error when handling event: {:?}", error)
        }
    }

    fn start_watching(&self) -> Result<()> {
        let (tx, event_channel) = channel();
        let mut watcher: RecommendedWatcher =
            Watcher::new(tx, Duration::from_millis(self.event_bounce_ms))
                .context("unable to create the fs watcher")?;
        for path in self.paths_to_watch.iter() {
            debug!("[local_file] watching {:?}", path);
            watcher
                .watch(path, RecursiveMode::Recursive)
                .context("fs watcher is unable to setup")?;
        }

        loop {
            match event_channel.recv() {
                Ok(event) => self.handle_event(event),
                Err(e) => panic!("FATAL ERROR with the channel: {:?}", e),
            }
        }
    }
}
