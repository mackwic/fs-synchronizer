use crate::redis_client::RedisClient;
use log::{debug, error};

pub struct FileEventHandler {
    _client: RedisClient,
}

impl FileEventHandler {
    pub fn new(client: RedisClient) -> FileEventHandler {
        FileEventHandler { _client: client }
    }

    pub fn handle_event(&self, event: notify::DebouncedEvent) {
        use notify::DebouncedEvent::*;

        debug!("got {:?}", event);

        match event {
            Create(_path) => (),
            Write(_path) => (),
            Remove(_path) => (),
            Rename(_old_path, _new_path) => (),
            NoticeWrite(_path) => (),  // do nothing
            NoticeRemove(_path) => (), // do nothing
            Chmod(_) => (),            // do nothing
            Rescan => debug!("rescanning watched paths"),
            Error(error, path) => error!("{} on path {:?}", error, path),
        }
    }
}
