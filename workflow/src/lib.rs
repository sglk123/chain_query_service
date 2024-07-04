use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::sync::mpsc;
use std::thread;
use lazy_static::lazy_static;
use tokio::sync::oneshot;
use chain_service::get_chain_service_config;
use pg_db_con::pg::{Block, get_pg_config, init, PgConfig};
use serde_json::{json, Value, Error};
use log::{error, info, log};

lazy_static! {
    static ref WORKFLOW: Mutex<Workflow> = Mutex::new(Workflow {
        tasks: HashMap::new(),
    });
}

struct Workflow {
    tasks: HashMap<String, Manager>,
}

struct Manager {
    manager_id: String,
    task_handle: Option<tokio::task::JoinHandle<()>>,
    stop_sender: Option<oneshot::Sender<()>>,
}

impl Manager {
    pub fn new(r: QueryRequest) -> Self {
        info!("create activity manager for {}",  &r.activity_name);
        let (stop_sender, stop_receiver) = oneshot::channel();
        let request = r.clone();
        let task_handle = tokio::spawn(async move {
            range_query(request, stop_receiver).await;
        });

        Manager {
            manager_id: r.activity_name,
            task_handle: Some(task_handle),
            stop_sender: Some(stop_sender),
        }
    }

    pub async fn stop_task(&mut self) {
        let sender = self.stop_sender.take();
        let handle = self.task_handle.take();

        if let Some(handle) = handle {
            if let Some(sender) = sender {
                if sender.send(()).is_ok() {
                    println!("Cancellation signal for task {} sent.", self.manager_id);
                }
                let _ = handle.await;
                println!("Task {} has been stopped and joined.", self.manager_id);
            }
        }
    }
}

#[derive(Clone, Debug)]
pub struct QueryRequest {
    pub activity_name: String,
    pub start_bn: String,
    pub end_block: String,
    pub chain_id: String,
}

#[derive(Clone, Debug)]
pub struct QueryEndRequest {
    pub activity_name: String,
    pub end_block: String,
}

pub async fn add_query_task(query_start: QueryRequest) {
    println!("receive add_task from{}", &query_start.activity_name);
    info!("receive add_task from {}",  &query_start.activity_name);
    let manager = Manager::new(query_start.clone());
    let mut wf = WORKFLOW.lock().unwrap();
    wf.tasks.insert(query_start.activity_name, manager);
}

pub async fn end_task(query_end: QueryEndRequest) {
    info!("receive end_task from {}",  &query_end.activity_name);
    let maybe_manager = {
        let mut wf = WORKFLOW.lock().unwrap();
        wf.tasks.remove(&query_end.activity_name)
    };

    if let Some(mut manager) = maybe_manager {
        manager.stop_task().await;
        println!("Task {} has ended", query_end.activity_name);
        info!("Task {} has ended",  &query_end.activity_name);
    } else {
        println!("Task not found");
        info!("Task {} not found",  &query_end.activity_name);
    }
}


async fn range_query(r: QueryRequest, mut stop_receiver: oneshot::Receiver<()>) {
    let start_bn = match r.start_bn.parse::<i64>() {
        Ok(num) => {
            if num < 0 {
                eprintln!("Start block number cannot be negative.");
                error!("Start block number cannot be negative for {:?}", &r.activity_name);
                return;
            }
            num
        }
        Err(_) => {
            eprintln!("Invalid start block number.");
            error!("Invalid start block number {:?}", &r.activity_name);
            return;
        }
    };

    let end_block = match r.end_block.parse::<i64>() {
        Ok(num) => num,
        Err(_) => {
            eprintln!("Invalid end block number.");
            error!("Invalid end block number {:?}", &r.activity_name);
            return;
        }
    };

    if end_block < start_bn {
        println!("timely query here");
        info!("end_block which is {:?} is smaller than start_block which is {:?},Scheduled query for {:?}",&r.end_block, &r.start_bn, &r.activity_name);
        let pg_config = get_pg_config().unwrap().clone();
        let db_pg = init(pg_config).await;
        db_pg.create_activity_table(r.activity_name.to_string()).await;
        let chain_config = get_chain_service_config().unwrap().clone();
        let chain_service_instance = chain_service::create_service(&chain_config.provider, &chain_config.endpoint).unwrap();
        let mut cur_timestamp = start_bn;
        // If end_block is less than start_bn, perform a timed query until stopped
        loop {
            tokio::select! {
                stop_signal = &mut stop_receiver => {
                if stop_signal.is_ok() {
                        info!("Received stop signal for task {}, shutting down...", &r.activity_name);
                        println!("Received stop signal for task {}, shutting down...", &r.activity_name);
                   match db_pg.drop_table(&r.activity_name).await {
                    Ok(_) => {
                                info!("drop successfully for {}", &r.activity_name);
                                println!("drop successfully for {}", &r.activity_name);
                    }
                    Err(_) => {
                                error!("failed to drop {}", &r.activity_name);
                                println!("failed to drop {}", &r.activity_name);
                    }
                }
                    break;
                } else {
                        info!("Stop signal channel was closed unexpectedly.need manual drop table for task {}", &r.activity_name);
                        println!("Stop signal channel was closed unexpectedly.need manual drop table for task {}", &r.activity_name);
                        break;
                }
            }
                _ = tokio::time::sleep(tokio::time::Duration::from_secs(30)) => {
                    println!("Performing periodic query from block {} to {} cur {},for task {:?}.", start_bn, end_block,cur_timestamp, &r.activity_name);
                    info!("Performing periodic query from block {} to {} cur {},for task {:?}.", start_bn, end_block,cur_timestamp, &r.activity_name);
                     let block_meta = chain_service_instance.get_block_by_number(&cur_timestamp.to_string()).await.unwrap();
                        db_pg.block_insert(Block {
                            block_num: cur_timestamp.to_string(),
                            event_meta: serde_json::to_vec(&block_meta).unwrap(),
                            }, &r.activity_name).await.unwrap();
                     cur_timestamp += 1;

                }
            }
        }
    } else {
        println!("Starting query from block {} to {}.", start_bn, end_block);
        info!("Starting query from block {} to {} for task {}.", start_bn, end_block, &r.activity_name);
        // create table
        let pg_config = get_pg_config().unwrap().clone();
        println!("{:?}", pg_config);
        // let test_conf = PgConfig {
        //     host: "localhost".to_string(),
        //     port: 5432,
        //     user: "postgres".to_string(),
        //     password: "123".to_string(),
        //     dbname: "mydb".to_string(),
        // };
        let db_pg = init(pg_config).await;
        db_pg.create_activity_table(r.activity_name.to_string()).await;
        let chain_config = get_chain_service_config().unwrap().clone();
        let chain_service_instance = chain_service::create_service(&chain_config.provider, &chain_config.endpoint).unwrap();
        // loop query
        println!("Starting loop query from block {} to {}.", start_bn, end_block);
        for block_number in start_bn..=end_block {
            println!("Starting query from block {}", block_number);
            info!("Starting query from block {} to {} cur {} for task {}.", start_bn, end_block,block_number, &r.activity_name);
            let block_meta = chain_service_instance.get_block_by_number(&block_number.to_string()).await.unwrap();
            db_pg.block_insert(Block {
                block_num: block_number.to_string(),
                event_meta: serde_json::to_vec(&block_meta).unwrap(),
            }, &r.activity_name).await.unwrap()
        }


        match stop_receiver.await {
            Ok(_) => {
                println!("Received stop signal, stopping the query.");
                info!("Received stop signal for task {}, shutting down...", &r.activity_name);
                match db_pg.drop_table(&r.activity_name).await {
                    Ok(_) => {
                        info!("drop successfully for {}", &r.activity_name);
                        println!("drop successfully for {}", r.activity_name);
                    }
                    Err(_) => {
                        error!("failed to drop {}", r.activity_name);
                        println!("failed to drop {}", r.activity_name);
                    }
                }
            }
            Err(_) => {
                error!("Stop signal channel was closed unexpectedly.Manual drop table {}", r.activity_name);
                println!("Stop signal channel was closed unexpectedly.Manual drop table {}", r.activity_name);
            }
        }
        println!("Query completed or stopped.");
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::{self, Duration};

    #[tokio::test]
    async fn test_add_and_end_task() {
        let test_query = QueryRequest {
            activity_name: "test_activity".to_string(),
            start_bn: "0".to_string(),
            end_block: "100".to_string(),
            chain_id: "ETH".to_string(),
        };

        add_query_task(test_query.clone()).await;

        {
            let wf = WORKFLOW.lock().unwrap();
            assert!(wf.tasks.contains_key(&test_query.activity_name), "Task should be present");
        }

        time::sleep(Duration::from_secs(2)).await;

        let end_request = QueryEndRequest {
            activity_name: test_query.activity_name.clone(),
            end_block: "100".to_string(),
        };

        end_task(end_request).await;

        {
            let wf = WORKFLOW.lock().unwrap();
            assert!(!wf.tasks.contains_key(&test_query.activity_name), "Task should be removed after ending it");
        }
    }
}
