use std::fmt::format;
use std::sync::Arc;
use bb8_postgres::{PostgresConnectionManager, tokio_postgres};
use bb8_postgres::bb8::{Pool, PooledConnection, RunError};
use postgres::NoTls;
use crate::{pg_insert, pg_select};
use tokio_postgres::Error;
use serde::{Deserialize, Serialize};
use log::{info, log};
use once_cell::sync::OnceCell;
use std::sync::RwLock;

type ConPool = Arc<Pool<PostgresConnectionManager<NoTls>>>;
type Con<'a> = PooledConnection<'a, PostgresConnectionManager<NoTls>>;

const POOL_SIZE: u32 = 15;

async fn create_database(conf: PgConfig) -> Result<(), Error> {
    let (client, connection) = tokio_postgres::connect(
        &format!("host={} user={} password={} port={}",
                 &conf.host, &conf.user, &conf.password, &conf.port),
        NoTls,
    )
        .await?;

    // Join connection future to not block
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    // Check if the database already exists
    let check_db_exists = format!(
        "SELECT 1 FROM pg_database WHERE datname='{}'",
        &conf.dbname
    );
    let rows = client.query(&check_db_exists, &[]).await?;

    if !rows.is_empty() {
        println!("Database '{}' already exists.", &conf.dbname);
        return Ok(());
    }

    let sql_command = format!("CREATE DATABASE {}", conf.dbname);
    client
        .execute(
            &sql_command,
            &[],
        )
        .await?;

    Ok(())
}

#[derive(Debug, Default, Clone)]
pub struct PgConfig {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: String,
    pub dbname: String,
}

static PG_CONFIG: OnceCell<RwLock<PgConfig>> = OnceCell::new();

pub fn initialize_pg_config(config: PgConfig) -> Result<(), &'static str> {
    info!("init pg: {:?}", config);
    PG_CONFIG.set(RwLock::new(config))
        .map_err(|_| "Config has already been initialized")
}

pub fn get_pg_config() -> Option<PgConfig> {
    PG_CONFIG.get()
        .and_then(|rw_lock| rw_lock.read().ok())
        .map(|config| config.clone())
}


// global db context
#[derive(Debug)]
pub struct DbPg {
    pub pool: ConPool,
    pub conf: PgConfig,
}

#[derive(Serialize, Deserialize, Clone, PartialEq, ::prost::Message)]
pub struct Block {
    #[prost(string, tag = 1)]
    pub block_num: String,
    #[prost(bytes, tag = 2)]
    pub event_meta: Vec<u8>,
}

pub async fn init(conf: PgConfig) -> DbPg {
    let mut pg_config = tokio_postgres::Config::new();
    pg_config.host(&conf.host);
    pg_config.port(conf.port);
    pg_config.user(&conf.user);
    pg_config.password(&conf.password);
    pg_config.dbname(&conf.dbname);

    let manager = PostgresConnectionManager::new(pg_config, NoTls);
    let pool = Arc::new(Pool::builder().max_size(POOL_SIZE).build(manager).await.unwrap());
    DbPg {
        pool,
        conf,
    }
}

impl DbPg {
    /// for block indexing in terms of block number
    pub async fn create_activity_table(&self, activity: String) {
        let mut conn = self.pool.get().await;
        match &conn {
            Ok(_) => {}
            Err(e) => { println!("{}", e); }
        }
        println!("create table {}", activity);
        let _ = conn.unwrap()
            .execute(&format!(
                "CREATE TABLE {}(
                id SERIAL PRIMARY KEY,
                block_number TEXT NOT NULL,
                block_meta BYTEA
            )", activity),
                     &[],
            )
            .await;
    }

    /// for transaction indexing
    pub async fn create_tx_table(&self, table_name: String) {
        let mut conn = self.pool.get().await;
        match &conn {
            Ok(_) => {}
            Err(e) => { println!("{}", e); }
        }
        println!("create table {}", table_name);
        let _ = conn.unwrap()
            .execute(&format!(
                "CREATE TABLE {}(
                id SERIAL PRIMARY KEY,
                tx_meta BYTEA
            )", table_name),
                     &[],
            )
            .await;
    }

    pub async fn table_exists(&self, table_name: &str) -> Result<bool, tokio_postgres::Error> {
        let conn = self.pool.get().await.unwrap();
        let query = format!("SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = '{}')", table_name);
        let rows = conn.query(&query, &[]).await?;
        Ok(rows[0].get(0))
    }

    pub async fn block_insert(&self, block: Block, table_name: &str) -> Result<(), tokio_postgres::Error> {
        let conn = self.pool.get().await.unwrap();
        let insert = format!("INSERT INTO {} (block_number, block_meta) VALUES ($1, $2)", table_name);
        conn.execute(&insert, &[&block.block_num, &block.event_meta]).await?;
        Ok(())
    }

    /// tx should be use serde_json::Value
    pub async fn insert_tx(&self, tx: Vec<u8>, table_name: &str) -> Result<(), tokio_postgres::Error> {
        let conn = self.pool.get().await.unwrap();
        let insert = format!("INSERT INTO {} (tx_meta) VALUES ($1)", table_name);
        conn.execute(&insert, &[&tx]).await?;
        Ok(())
    }


    pub async fn drop_table(&self, table_name: &str) -> Result<(), tokio_postgres::Error> {
        let conn = self.pool.get().await.unwrap();
        let query = format!("DROP TABLE IF EXISTS {}", table_name);
        conn.execute(&query, &[]).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn block_insert() {
        // Define a test configuration
        let test_conf = PgConfig {
            host: "localhost".to_string(),
            port: 5432,
            user: "postgres".to_string(),
            password: "123".to_string(),
            dbname: "sglk_sample1".to_string(),
        };
        let db_pg = init(test_conf).await;
        db_pg.block_insert(Block {
            block_num: "1".to_string(),
            event_meta: vec![1, 2, 3],
        }, "activity1").await.expect("fail to insert");
    }

    #[tokio::test]
    async fn test_create_table() {
        // Define a test configuration
        let test_conf = PgConfig {
            host: "52.221.181.98".to_string(),
            port: 5432,
            user: "postgres".to_string(),
            password: "postgres".to_string(),
            dbname: "sglk".to_string(),
        };
        let db_pg = init(test_conf).await;
        db_pg.create_activity_table("activity1".to_string()).await;
    }

    #[tokio::test]
    async fn test_drop_table() {
        let test_conf = PgConfig {
            host: "localhost".to_string(),
            port: 5432,
            user: "postgres".to_string(),
            password: "123".to_string(),
            dbname: "sglk_sample1".to_string(),
        };
        let db_pg = init(test_conf).await;

        let result = db_pg.drop_table("activity1").await;

        assert!(result.is_ok(), "Failed to drop table: {:?}", result.err());
    }


    #[tokio::test]
    async fn test_create_tx_table() {
        let test_conf = PgConfig {
            host: "localhost".to_string(),
            port: 5432,
            user: "postgres".to_string(),
            password: "123".to_string(),
            dbname: "mydb".to_string(),
        };
        let db_pg = init(test_conf).await;
        db_pg.create_tx_table("tx_table".to_string()).await;
    }

    #[tokio::test]
    async fn test_insert_tx() {
        let test_conf = PgConfig {
            host: "localhost".to_string(),
            port: 5432,
            user: "postgres".to_string(),
            password: "123".to_string(),
            dbname: "mydb".to_string(),
        };
        let db_pg = init(test_conf).await;
        let tx_meta = vec![1, 2, 3];
        db_pg.insert_tx(tx_meta, "tx_table").await.expect("fail to insert tx");
    }


    #[tokio::test]
    async fn test_table_exists() {
        let test_conf = PgConfig {
            host: "localhost".to_string(),
            port: 5432,
            user: "postgres".to_string(),
            password: "123".to_string(),
            dbname: "mydb".to_string(),
        };
        let db_pg = init(test_conf).await;

        let exists = db_pg.table_exists("users").await.expect("Failed to check if table exists");
        println!("is exists {:?}", exists);
        assert!(exists, "Table should exist");
    }
}
