mod log;
mod error;
mod runtime;

use std::path::{Path, PathBuf};
use ::log::info;
use serde::{Deserialize, Serialize};
use tokio;
use structopt::StructOpt;
use chain_service::{ChainServiceConfig, initialize_chain_service, initialize_service};
use pg_db_con::pg::{initialize_pg_config, PgConfig};
use rpc::{MyQueryService, ServerConfig};
use crate::error::{ConfigError, ConfigResult};
use crate::log::init_log_with_default;
use crate::runtime::takio_runtime::block_forever_on;


#[tokio::main]
async fn main() {
    block_forever_on(async_main());
}

async fn async_main() {
    init_log_with_default();
    let args = Cli::from_args();
    let config = load_config(args.config_path.clone()).unwrap();
    println!("Config: {:?}", config);
    info!("Config: {:?}", config);
    let rpc_config = ServerConfig {
        address: config.rpc.address.to_string(),
    };
    let service = MyQueryService;
    initialize_pg_config(PgConfig {
        host: config.db.host.to_string(),
        port: config.db.port,
        user: config.db.user.to_string(),
        password: config.db.password.to_string(),
        dbname: config.db.dbname.to_string(),
    }).unwrap();
    initialize_chain_service(
        ChainServiceConfig {
            provider: config.chain_service.provider.to_string(),
            endpoint: config.chain_service.endpoint.to_string(),
            maxcredit: config.chain_service.maxcredit,
        }
    ).unwrap();
    initialize_service(ChainServiceConfig {
        provider: config.chain_service.provider.to_string(),
        endpoint: config.chain_service.endpoint.to_string(),
        maxcredit: config.chain_service.maxcredit,
    }).unwrap();
    rpc::init(service, rpc_config).await.unwrap()
}

#[derive(StructOpt)]
struct Cli {
    #[structopt(short = "c", long = "config", parse(from_os_str), help = "Yaml file only")]
    config_path: std::path::PathBuf,
}

fn load_config(path: PathBuf) -> ConfigResult<NodeConfig> {
    let p: &Path = path.as_ref();
    let config_yaml = std::fs::read_to_string(p).map_err(|err| match err {
        e @ std::io::Error { .. } if e.kind() == std::io::ErrorKind::NotFound => {
            ConfigError::ConfigMissing(path.into())
        }
        _ => err.into(),
    })?;
    serde_yaml::from_str(&config_yaml).map_err(ConfigError::SerializationError)
}

#[derive(Clone, Deserialize, Serialize, Debug, Default)]
pub struct NodeConfig {
    pub chain_service: ChainServiceConf,
    pub rpc: RpcConfig,
    pub db: PgDbConfig,
}

#[derive(Clone, Deserialize, Serialize, Debug, Default)]
pub struct ChainServiceConf {
    pub provider: String,
    pub endpoint: String,
    pub maxcredit: usize,
}

#[derive(Clone, Deserialize, Serialize, Debug, Default)]
pub struct RpcConfig {
    pub address: String,
}

#[derive(Clone, Deserialize, Serialize, Debug, Default)]
pub struct PgDbConfig {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: String,
    pub dbname: String,
}