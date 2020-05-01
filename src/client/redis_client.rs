use anyhow::{bail, Context, Result};
use log::debug;
use r2d2_redis::{r2d2, redis, RedisConnectionManager};

type RedisConnection = r2d2::PooledConnection<r2d2_redis::RedisConnectionManager>;
type RedisPool = r2d2::Pool<r2d2_redis::RedisConnectionManager>;

#[derive(Debug, Clone)]
pub struct RedisClient {
    pub redis_url: String,
    connection_pool: RedisPool,
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

    pub fn get(&self, key: &str) -> Result<Vec<u8>> {
        debug!("[redis_client] sending GET {}", key);
        let mut connection = self.take_connection()?;
        let bytes = redis::cmd("GET")
            .arg(key)
            .query::<Vec<u8>>(&mut *connection)
            .context("error during the Redis GET query")?;
        Ok(bytes)
    }

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

    pub fn remove(&self, key: &str) -> Result<(), anyhow::Error> {
        debug!("[redis_client] sending DEL {}", key);
        let mut connection = self.take_connection()?;
        redis::cmd("DEL")
            .arg(key)
            .query::<()>(&mut *connection)
            .context("error during the Redis DEL query")?;
        Ok(())
    }

    pub fn publish(&self, channel: &str, message: &str) -> Result<()> {
        debug!("[redis_client] sending PUBLISH {} {}", channel, message);
        let mut connection = self.take_connection()?;
        redis::cmd("PUBLISH")
            .arg(channel)
            .arg(message)
            .query(&mut *connection)
            .context("error during the Redis PUBLISH query")?;
        Ok(())
    }

    pub fn take_connection(&self) -> Result<RedisConnection> {
        let connection = self
            .connection_pool
            .get()
            .context("unable to get redis connection")?;
        Ok(connection)
    }

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
