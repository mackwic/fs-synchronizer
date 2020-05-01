use crate::client::redis_client::RedisClient;
use log::{debug, error};

pub struct RemoteFileEventHandler {
    client: RedisClient,
}

impl RemoteFileEventHandler {
    pub fn new(client: RedisClient) -> RemoteFileEventHandler {
        RemoteFileEventHandler { client: client }
    }

    pub fn handle_event(&self, event: notify::DebouncedEvent) {
        use notify::DebouncedEvent::*;

        debug!("[remote_file] got {:?}", event);

        match event {
            Create(_path) => (),
            Write(_path) => (),
            Remove(_path) => (),
            Rename(_old_path, _new_path) => (),
            NoticeWrite(_path) => (),  // do nothing
            NoticeRemove(_path) => (), // do nothing
            Chmod(_) => (),            // do nothing
            Rescan => debug!("[remote_file] rescanning watched paths"),
            Error(error, path) => error!("{} on path {:?}", error, path),
        }
    }
}
