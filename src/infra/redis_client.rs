use anyhow::{bail, Context, Result};
use std::time::Duration;

pub struct RedisClient {
    pub redis_url: String,
    inner_client: redis::Client,
}

impl RedisClient {
    /// Create new client, ensuring that the connection to the redis server is OK
    pub fn new(redis_url: String) -> Result<RedisClient> {
        let inner_client: redis::Client =
            redis::Client::open(redis_url).context("Invalid Redis URL")?;
        let mut connection: redis::Connection = inner_client
            .get_connection_with_timeout(Duration::from_secs(1))
            .context("Unable to get connection to Redis")?;

        let response: String = redis::cmd("PING")
            .query::<String>(&mut connection)
            .context("Unable to ping Redis")?;

        if response != "PONG" {
            bail!("Redis server did not answered with PONG")
        }

        let client = RedisClient {
            redis_url,
            inner_client,
        };
        Ok(client)
    }
}
