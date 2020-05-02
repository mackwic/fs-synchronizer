use anyhow::{anyhow, bail, Context, Result};
use log::{debug, error};
use r2d2_redis::{r2d2, redis, RedisConnectionManager};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

type RedisConnection = r2d2::PooledConnection<r2d2_redis::RedisConnectionManager>;
type RedisPool = r2d2::Pool<r2d2_redis::RedisConnectionManager>;

#[derive(Debug, Clone)]
pub struct RedisClient {
    pub redis_url: String,
    connection_pool: RedisPool,
}

#[derive(Debug, PartialEq, Deserialize, Serialize)]
pub enum RedisPublishPayload {
    /// Emitter id, then Path
    OnePathMessage(u64, PathBuf),
    /// Emitter id, then Path, and Path
    TwoPathMessage(u64, PathBuf, PathBuf),
}

impl RedisPublishPayload {
    pub fn get_emitter_id(&self) -> u64 {
        use RedisPublishPayload::*;
        match self {
            OnePathMessage(emitter_id, _) | TwoPathMessage(emitter_id, _, _) => *emitter_id,
        }
    }
}

impl RedisClient {
    /// Create new client, ensuring that the connection to the redis server is OK
    pub fn new(redis_url: String) -> Result<RedisClient> {
        const DEFAULT_POOL_SIZE: u32 = 15;

        let manager =
            RedisConnectionManager::new(redis_url.clone()).context("Invalid Redis URL")?;
        let connection_pool: r2d2::Pool<_> = r2d2::Pool::builder()
            .max_size(DEFAULT_POOL_SIZE)
            .build(manager)
            .context("Unable to create the connexion pool")?;

        let mut connection = connection_pool.get().unwrap();
        RedisClient::ping_server(&mut *connection)?;

        let client = RedisClient {
            redis_url,
            connection_pool,
        };
        Ok(client)
    }

    /// run redis SET command: set a key to a value
    pub fn set(&self, key: &str, value: &[u8]) -> Result<()> {
        debug!("[redis_client] sending SET {} <value>", key);
        let mut connection = self.take_connection()?;
        redis::cmd("SET")
            .arg(key)
            .arg(value)
            .query(&mut *connection)
            .context("error during the Redis SET query")?;
        Ok(())
    }

    /// run redis GET command: get the value of a key
    pub fn get(&self, key: &str) -> Result<Vec<u8>> {
        debug!("[redis_client] sending GET {}", key);
        let mut connection = self.take_connection()?;
        let bytes = redis::cmd("GET")
            .arg(key)
            .query::<Vec<u8>>(&mut *connection)
            .context("error during the Redis GET query")?;
        Ok(bytes)
    }

    /// run redis RENAME command: change a key
    pub fn rename(&self, old_key: &str, new_key: &str) -> Result<(), anyhow::Error> {
        debug!("[redis_client] sending RENAME {} {}", old_key, new_key);
        let mut connection = self.take_connection()?;
        redis::cmd("RENAME")
            .arg(old_key)
            .arg(new_key)
            .query::<()>(&mut *connection)
            .context("error during the Redis RENAME query")?;
        Ok(())
    }

    /// run redis DEL command: remove the key/value pair
    pub fn remove(&self, key: &str) -> Result<(), anyhow::Error> {
        debug!("[redis_client] sending DEL {}", key);
        let mut connection = self.take_connection()?;
        redis::cmd("DEL")
            .arg(key)
            .query::<()>(&mut *connection)
            .context("error during the Redis DEL query")?;
        Ok(())
    }

    /// run redis PUBLISH command: publish an event on the given channel
    pub fn publish(&self, channel: &str, message: RedisPublishPayload) -> Result<()> {
        debug!("[redis_client] sending PUBLISH {} {:?}", channel, message);
        let mut connection = self.take_connection()?;
        redis::cmd("PUBLISH")
            .arg(channel)
            .arg(rmp_serde::to_vec(&message).expect(
                "messagepack serialization of RedisPublishPayload messages should never fail",
            ))
            .query(&mut *connection)
            .context("error during the Redis PUBLISH query")?;
        Ok(())
    }

    /// run redis SADD command: add a member to a set
    pub fn sadd(&self, set: &str, member_key: &str) -> Result<()> {
        debug!("[redis_client] sending SADD {} {}", set, member_key);
        let mut connection = self.take_connection()?;
        redis::cmd("SADD")
            .arg(set)
            .arg(member_key)
            .query::<()>(&mut *connection)
            .context("error during the Redis SADD query")?;
        Ok(())
    }

    /// run redis SREM command: remove a member to a set
    pub fn srem(&self, set: &str, member_key: &str) -> Result<()> {
        debug!("[redis_client] sending SREM {} {}", set, member_key);
        let mut connection = self.take_connection()?;
        redis::cmd("SREM")
            .arg(set)
            .arg(member_key)
            .query::<()>(&mut *connection)
            .context("error during the Redis SREM query")?;
        Ok(())
    }

    /// run redis SMOVE command: change a member name in a set
    pub fn smove(&self, set: &str, old_member_key: &str, new_member_key: &str) -> Result<()> {
        debug!(
            "[redis_client] sending SMOVE {} {} {}",
            set, old_member_key, new_member_key
        );
        let mut connection = self.take_connection()?;
        redis::cmd("SMOVE")
            .arg(set)
            .arg(old_member_key)
            .arg(new_member_key)
            .query::<()>(&mut *connection)
            .context("error during the Redis SMOVE query")?;
        Ok(())
    }
    /// run redis MULTI command: open a new transaction
    pub fn multi(&self) -> Result<()> {
        debug!("[redis_client] sending MULTI (new transaction)",);
        let mut connection = self.take_connection()?;
        redis::cmd("MULTI")
            .query::<()>(&mut *connection)
            .context("error during the Redis MULTI query")?;
        Ok(())
    }

    /// run redis EXEC command: execute the opened transaction
    pub fn exec(&self) -> Result<()> {
        debug!("[redis_client] sending EXEC (resolve current transaction)",);
        let mut connection = self.take_connection()?;
        redis::cmd("EXEC")
            .query::<()>(&mut *connection)
            .context("error during the Redis EXEC query")?;
        Ok(())
    }

    /// run redis DISCARD command: discard the opened transaction
    pub fn discard(&self) -> Result<()> {
        debug!("[redis_client] sending DISCARD (discard current transaction)",);
        let mut connection = self.take_connection()?;
        redis::cmd("DISCARD")
            .query::<()>(&mut *connection)
            .context("error during the Redis DISCARD query")?;
        Ok(())
    }

    /// take a connection from the pool
    pub fn take_connection(&self) -> Result<RedisConnection> {
        let connection = self
            .connection_pool
            .get()
            .context("unable to get redis connection")?;
        Ok(connection)
    }

    pub fn in_transaction(&self, commands: impl FnOnce() -> Result<()>) -> Result<()> {
        self.multi()?;

        let res = commands();
        if let Err(error) = res {
            error!(
                "Error during in Redis commands: {:?}. Cancel the transaction.",
                error
            );
            self.discard()?;
            Err(anyhow!(
                "transaction was cancelled because of the following error: {:?}",
                error
            ))
        } else {
            self.exec()?;
            Ok(())
        }
    }

    /// run a PING command to the senver and ensure it respond with PONG
    fn ping_server(connection: &mut dyn r2d2_redis::redis::ConnectionLike) -> Result<()> {
        let response: String = r2d2_redis::redis::cmd("PING")
            .query::<String>(connection)
            .context("Unable to ping Redis")?;

        if response != "PONG" {
            bail!("Redis server did not answered with PONG")
        } else {
            Ok(())
        }
    }
}
