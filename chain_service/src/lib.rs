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
    async fn get_transaction_by_hash(&self, tx_hash: &str) -> Result<Value, Box<dyn std::error::Error>>;
    async fn get_tx_from_block(&self, block_number: &str, address: &str) -> Result<Vec<Value>, Box<dyn std::error::Error>>;
    async fn get_tx_list_from_block(&self, block_number: &str, address: &str) -> Result<Vec<Value>, Box<dyn std::error::Error>>;

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
            false
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

    async fn get_transaction_by_hash(&self, tx_hash: &str) -> Result<Value, Box<dyn std::error::Error>> {
        let params = json!([tx_hash]);
        let body = json!({
            "method": "eth_getTransactionByHash",
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

    async fn get_tx_from_block(&self, block_number: &str, address: &str) -> Result<Vec<Value>, Box<dyn std::error::Error>> {
        let block = self.get_block_by_number(block_number).await?;
        let tx_hashes = block["result"]["transactions"].as_array().ok_or("No transactions found")?;
      //  println!("tx hashes from block [{}] are [{}]", block_number,  serde_json::to_string(&tx_hashes)?);
        let mut transactions = Vec::new();
        for tx_hash in tx_hashes {
            if let Some(tx_hash_str) = tx_hash.as_str() {
                let transaction = self.get_transaction_by_hash(tx_hash_str).await?;
             //   println!("block [{}] txs has [{}]", block_number, &transaction);
                if transaction["result"]["to"] == address {
                    transactions.push(transaction["result"].clone());
                }
            }
        }
        Ok(transactions)
    }

    async fn get_tx_list_from_block(&self, block_number: &str, address: &str) -> Result<Vec<Value>, Box<dyn std::error::Error>> {
        let params = json!([
            block_number,
            true
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

        let block = response.json::<Value>().await?;
        let timestamp = block["result"]["timestamp"].clone();
        let error_vec = vec![];
        let transactions = block["result"]["transactions"].as_array().unwrap_or(&error_vec);
        let mut filtered_txs: Vec<Value> = Vec::new();

        for tx in transactions {
            if let Some(to_address) = tx["to"].as_str() {
                if to_address.eq_ignore_ascii_case(address) {
                    let mut tx_with_timestamp = tx.clone();
                    tx_with_timestamp["timestamp"] = timestamp.clone();
                    filtered_txs.push(tx_with_timestamp);
                }
            }
        }

        Ok(filtered_txs)
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
        let response = service.get_block_by_number("0xc5043f").await.unwrap();
        println!("Block data: {:?}", response);
        // assert_eq!(response["result"]["number"], "0x10");
    }

    #[tokio::test]
    async fn test_get_tx_from_block() {
        let service = create_service("quicknode", "https://docs-demo.quiknode.pro/").unwrap();
        let address = "0xa57bd00134b2850b2a1c55860c9e9ea100fdd6cf";
        let transactions = service.get_tx_from_block("0xc5043f", address).await.unwrap();
        println!("Transactions to address {}: {:?}", address, transactions);
    }

    #[tokio::test]
    async fn test_get_tx_list_from_block() {
        let service = create_service("quicknode", "https://docs-demo.quiknode.pro/").unwrap();
        let address = "0xa57bd00134b2850b2a1c55860c9e9ea100fdd6cf";
        let transactions = service.get_tx_list_from_block("0xc5043f", address).await.unwrap();
        println!("Transactions to address {}: {:?}", address, transactions);
    }
}

// json tx format EIP-2930 && EIP-1559
// {
// "accessList": [
// {
// "address": "0x...",
// "storageKeys": ["0x...", "0x..."]
// }
// ],
// "blockHash": "0x...",
// "blockNumber": "0x...",
// "chainId": "0x...",
// "from": "0x...",
// "gas": "0x...",
// "gasPrice": "0x...",
// "hash": "0x...",
// "input": "0x...",
// "nonce": "0x...",
// "r": "0x...",
// "s": "0x...",
// "to": "0x...",
// "transactionIndex": "0x...",
// "type": "0x1",
// "v": "0x...",
// "value": "0x...",
// "yParity": "0x..."
// }