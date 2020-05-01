use crate::store::redis_store::RedisStore;
use anyhow::anyhow;
use log::{debug, error};

pub struct LocalFileEventHandler {
    store: RedisStore,
}

impl LocalFileEventHandler {
    pub fn new(store: RedisStore) -> LocalFileEventHandler {
        LocalFileEventHandler { store }
    }

    pub fn handle_event(&self, event: notify::DebouncedEvent) {
        use notify::DebouncedEvent::*;

        debug!("got {:?}", event);

        let res = match event {
            Create(path) => self.store.new_file(path),
            Write(_path) => Ok(()),
            Remove(_path) => Ok(()),
            Rename(_old_path, _new_path) => Ok(()),
            NoticeWrite(_path) => Ok(()),  // do nothing
            NoticeRemove(_path) => Ok(()), // do nothing
            Chmod(_) => Ok(()),            // do nothing
            Rescan => {
                debug!("rescanning watched paths");
                Ok(())
            }
            Error(error, path) => Err(anyhow!("Error: {} on path {:?}", error, path)),
        };

        if let Err(error) = res {
            error!("Error when handling event: {:?}", error)
        }
    }
}
