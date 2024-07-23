use std::sync::Arc;
use tokio::sync::{Mutex, Semaphore};
use reqwest;
use serde_json::{json, Value};
use async_trait::async_trait;
use once_cell::sync::OnceCell;
use std::sync::RwLock;
use log::info;
use tokio::time::{self, Instant, sleep};
use std::time::Duration;


#[async_trait]
pub trait BlockchainQueryService: Send + Sync {
    async fn get_block_by_number(&self, block_number: &str) -> Result<Value, Box<dyn std::error::Error + Send + Sync>>;
    async fn get_transaction_by_hash(&self, tx_hash: &str) -> Result<Value, Box<dyn std::error::Error>>;
    //    async fn get_tx_from_block(&self, block_number: &str, address: &str) -> Result<Vec<Value>, Box<dyn std::error::Error>>;
    async fn get_tx_list_from_block(&self, block_number: &str, address: &str) -> Result<Vec<Value>, Box<dyn std::error::Error>>;
}

struct CreditBasedRateLimiter {
    semaphore: Arc<Semaphore>,
    max_credits: usize,
    refill_interval: Duration,
}

impl CreditBasedRateLimiter {
    fn new(max_credits: usize, refill_interval: Duration) -> Self {
        let semaphore = Arc::new(Semaphore::new(max_credits));
        let limiter = CreditBasedRateLimiter {
            semaphore: semaphore.clone(),
            max_credits,
            refill_interval,
        };
        tokio::spawn(async move {
            let mut interval = time::interval(refill_interval);
            loop {
                interval.tick().await;
                let current_permits = semaphore.available_permits();
                println!("permit refresh {}", max_credits - current_permits);
                semaphore.add_permits(max_credits - current_permits);
            }
        });

        limiter
    }

    async fn acquire(&self, credits: usize) {
        self.semaphore.acquire_many(credits as u32).await.unwrap().forget();
    }
}

struct QuickNodeService {
    client: reqwest::Client,
    endpoint: String,
    rate_limiter: Arc<CreditBasedRateLimiter>,
}

impl QuickNodeService {
    fn new(endpoint: &str, rate_limiter: Arc<CreditBasedRateLimiter>) -> Self {
        QuickNodeService {
            client: reqwest::Client::new(),
            endpoint: endpoint.to_string(),
            rate_limiter,
        }
    }
}

#[async_trait]
impl BlockchainQueryService for QuickNodeService {
    async fn get_block_by_number(&self, block_number: &str) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
        self.rate_limiter.acquire(20).await;
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
        if response_json.get("code").map_or(false, |code| code.as_i64() == Some(-32007)) {
            println!("Rate limit error: {}. Retrying after 1 second...", response_json["message"]);
            sleep(Duration::from_secs(1)).await;
            // Retry
            return self.get_block_by_number(block_number).await;
        }
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

    // async fn get_tx_from_block(&self, block_number: &str, address: &str) -> Result<Vec<Value>, Box<dyn std::error::Error>> {
    //     let block = self.get_block_by_number(block_number).await?;
    //     let tx_hashes = block["result"]["transactions"].as_array().ok_or("No transactions found")?;
    //   //  println!("tx hashes from block [{}] are [{}]", block_number,  serde_json::to_string(&tx_hashes)?);
    //     let mut transactions = Vec::new();
    //     for tx_hash in tx_hashes {
    //         if let Some(tx_hash_str) = tx_hash.as_str() {
    //             let transaction = self.get_transaction_by_hash(tx_hash_str).await?;
    //          //   println!("block [{}] txs has [{}]", block_number, &transaction);
    //             if transaction["result"]["to"] == address {
    //                 transactions.push(transaction["result"].clone());
    //             }
    //         }
    //     }
    //     Ok(transactions)
    // }

    async fn get_tx_list_from_block(&self, block_number: &str, address: &str) -> Result<Vec<Value>, Box<dyn std::error::Error>> {
        self.rate_limiter.acquire(20).await;
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
        println!("response: {:?}", &response);
        let block = response.json::<Value>().await?;
        println!("block is {:?}", &block);
        if block.get("code").map_or(false, |code| code.as_i64() == Some(-32007)) {
            println!("Rate limit error: {}. Retrying after 1 second...", block["message"]);
            sleep(Duration::from_secs(1)).await;
            // Retry
            return self.get_tx_list_from_block(block_number, address).await;
        }

        let timestamp = block["result"]["timestamp"].clone();
        let error_vec = vec![];
        let transactions = block["result"]["transactions"].as_array().unwrap_or(&error_vec);
        let mut filtered_txs: Vec<Value> = Vec::new();
        // println!("get tx from addr {}", address);
        // println!("tx is {:?}", transactions);
        for tx in transactions {
            if let Some(to_address) = tx["to"].as_str() {
                //  println!("check addr for {}, match with address {}",to_address, address);
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
    let rate_limiter = Arc::new(CreditBasedRateLimiter::new(350, Duration::from_secs(1)));
    match provider {
        "quicknode" => Ok(Arc::new(QuickNodeService::new(endpoint, rate_limiter))),
        _ => Err("No such 3rd party service")
    }
}

static SERVICE_INSTANCE: OnceCell<Arc<dyn BlockchainQueryService>> = OnceCell::new();

pub fn initialize_service(config: ChainServiceConfig) -> Result<(), &'static str> {
    let rate_limiter = Arc::new(CreditBasedRateLimiter::new(config.maxcredit, Duration::from_secs(1)));
    let service: Arc<dyn BlockchainQueryService> = match config.provider.as_str() {
        "quicknode" => Arc::new(QuickNodeService::new(&config.endpoint, rate_limiter)),
        _ => return Err("No such 3rd party service"),
    };

    SERVICE_INSTANCE.set(service).map_err(|_| "Service has already been initialized")
}

pub fn get_service() -> Arc<dyn BlockchainQueryService> {
    SERVICE_INSTANCE.get().expect("Service not initialized").clone()
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
    pub maxcredit: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::task;
    use futures::future::join_all;

    #[tokio::test]
    async fn test_get_block_height() {
        let service = create_service("quicknode", "https://docs-demo.quiknode.pro/").unwrap();
        //20305518 0x135D66E 0xc5043f
        let response = service.get_block_by_number("0x135D66E").await.unwrap();
        println!("Block data: {:?}", response);
        // assert_eq!(response["result"]["number"], "0x10");
    }

    // #[tokio::test]
    // async fn test_get_tx_from_block() {
    //     let service = create_service("quicknode", "https://docs-demo.quiknode.pro/").unwrap();
    //     let address = "0xa57bd00134b2850b2a1c55860c9e9ea100fdd6cf";
    //     let transactions = service.get_tx_from_block("0xc5043f", address).await.unwrap();
    //     println!("Transactions to address {}: {:?}", address, transactions);
    // }

    #[tokio::test]
    async fn test_get_tx_list_from_block() {
        let service = create_service("quicknode", "https://thrumming-chaotic-patron.quiknode.pro/57c634bd1ea0b5cd640c816519f781ef833d7c5c/").unwrap();
        let address = "0xdAC17F958D2ee523a2206206994597C13D831ec7";
        let transactions = service.get_tx_list_from_block("0x135D66E", address).await.unwrap();
        println!("Transactions to address {}: {:?}", address, transactions);
    }

    #[tokio::test]
    async fn test_concurrent_requests() {
        let config = ChainServiceConfig {
            provider: "quicknode".to_string(),
            endpoint: "https://docs-demo.quiknode.pro/".to_string(),
            maxcredit: 100,
        };

        initialize_service(config).unwrap();

        let mut handles = Vec::with_capacity(14);
        for _ in 0..14 {
            let service = get_service().clone();
            handles.push(task::spawn(async move {
                service.get_block_by_number("0x135D66E").await
            }));
        }

        let results = join_all(handles).await;

        for result in results {
            match result {
                Ok(Ok(response)) => println!("Block data: {:?}", response),
                Ok(Err(e)) => println!("Request error: {:?}", e),
                Err(e) => println!("Task error: {:?}", e),
            }
        }
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