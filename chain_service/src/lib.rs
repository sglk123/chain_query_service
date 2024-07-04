use std::sync::Arc;
use tokio::sync::Mutex;
use reqwest;
use serde_json::{json, Value};
use async_trait::async_trait;
use once_cell::sync::OnceCell;
use std::sync::RwLock;
use log::info;


#[async_trait]
pub trait BlockchainQueryService: Send + Sync {
    async fn get_block_by_number(&self, block_number: &str) -> Result<Value, Box<dyn std::error::Error>>;
}


struct QuickNodeService {
    client: reqwest::Client,
    endpoint: String,
}

impl QuickNodeService {
    fn new(endpoint: &str) -> Self {
        QuickNodeService {
            client: reqwest::Client::new(),
            endpoint: endpoint.to_string(),
        }
    }
}

#[async_trait]
impl BlockchainQueryService for QuickNodeService {
    async fn get_block_by_number(&self, block_number: &str) -> Result<Value, Box<dyn std::error::Error>> {
        let params = json!([
            block_number,
            true          // Set to 'true' to retrieve the full transaction data
        ]);
        let body = json!({
            "method": "eth_getBlockByNumber",
            "params": params,
            "id": 1,
            "jsonrpc": "2.0"
        });
        let response = self.client.clone().post(&self.endpoint)
            .header("Content-Type", "application/json")
            .json(&body)
            .send()
            .await?;

        let response_json: Value = response.json().await?;
        Ok(response_json)
    }
}

pub fn create_service(provider: &str, endpoint: &str) -> Result<Arc<dyn BlockchainQueryService>, &'static str> {
    match provider {
        "quicknode" => Ok(Arc::new(QuickNodeService::new(endpoint))),
        _ => Err("No such 3rd party service")
    }
}

static CHAIN_SERVICE_CONFIG: OnceCell<RwLock<ChainServiceConfig>> = OnceCell::new();

pub fn initialize_chain_service(config: ChainServiceConfig) -> Result<(), &'static str> {
    info!("init chain service: {:?}", config);
    CHAIN_SERVICE_CONFIG.set(RwLock::new(config))
        .map_err(|_| "Config has already been initialized")
}

pub fn get_chain_service_config() -> Option<ChainServiceConfig> {
    CHAIN_SERVICE_CONFIG.get()
        .and_then(|rw_lock| rw_lock.read().ok())
        .map(|config| config.clone())
}

#[derive(Debug, Default, Clone)]
pub struct ChainServiceConfig {
    pub provider: String,
    pub endpoint: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_get_block_height() {
        let service = create_service("quicknode", "https://docs-demo.quiknode.pro/").unwrap();
        let response = service.get_block_by_number("0x10").await.unwrap();
        println!("Block data: {:?}", response);
        assert_eq!(response["result"]["number"], "0x10");
    }
}
