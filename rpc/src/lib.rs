use crate::query::query_service_server::{QueryService, QueryServiceServer};
use crate::query::{QueryEndRequest, QueryRequest, QueryResponse, TxQueryRequest};
use tonic::transport::Server;
use std::net::SocketAddr;
use log::info;
use tonic::{Request, Response, Status};
use workflow::{add_query_task, add_tx_query_task, end_task, should_start};

mod query;

#[cfg(test)]
mod tests {
    use tonic_build;
    use crate::{init, MyQueryService, ServerConfig};
    use tokio::time::{timeout, Duration};
    use tonic::transport::Channel;
    use crate::query::query_service_client::QueryServiceClient;
    use crate::query::{QueryEndRequest, QueryRequest, TxQueryRequest};

    #[test]
    fn test_proto_compilation() -> Result<(), Box<dyn std::error::Error>> {
        tonic_build::configure()
            .build_server(true)
            .build_client(true)
            .out_dir("src")
            .compile(&["src/service.proto"], &["proto/"])?;
        Ok(())
    }

    #[tokio::test]
    async fn test_init() -> Result<(), Box<dyn std::error::Error>> {
        let config = ServerConfig::default();
        let service = MyQueryService;

        let server_handle = tokio::spawn(async move {
            init(service, config).await
        });

        let test_result = timeout(Duration::from_secs(5), server_handle).await;
        match test_result {
            Ok(result) => result.unwrap(),
            Err(_) => Ok(println!("Server did not shut down in time"))
        }.expect("panic");

        Ok(())
    }

    #[tokio::test]
    async fn test_client() -> Result<(), Box<dyn std::error::Error>> {
        let channel = Channel::from_static("http://45.77.43.70:50051")
            .connect()
            .await?;

        let mut client = QueryServiceClient::new(channel);

        let request = tonic::Request::new(QueryRequest {
            activity_name: "Activity2".to_string(),
            start_bn: "100".to_string(),
            end_block: "105".to_string(),
            chain_id: "ETH".to_string(),
        });

        let response = client.start_query(request).await?;
        println!("RESPONSE={:?}", response.into_inner().message);

        // let end_request = tonic::Request::new(QueryEndRequest {
        //     activity_name: "Activity1".to_string(),
        //     end_block: "103".to_string(),
        // });
        // let end_response = client.end_query(end_request).await?;
        // println!("RESPONSE={:?}", end_response.into_inner().message);

        Ok(())
    }

    #[tokio::test]
    async fn test_tx_client() -> Result<(), Box<dyn std::error::Error>> {
        let channel = Channel::from_static("http://18.140.5.197:50051")
            .connect()
            .await?;

        // let channel = Channel::from_static("http://localhost:50051")
        //     .connect()
        //     .await?;

        let mut client = QueryServiceClient::new(channel);
        //
        // // // 0xc5043f contain this tx
        // let request = tonic::Request::new(TxQueryRequest {
        //     activity_name: "Activity672".to_string(),
        //     start_bn: "12900287".to_string(),
        //     end_block: "12900295".to_string(),
        //     chain_id: "ETH".to_string(),
        //     address: "0xa57bd00134b2850b2a1c55860c9e9ea100fdd6cf".to_string(),
        // });

        let request = tonic::Request::new(TxQueryRequest {
            activity_name: "Activitysss".to_string(),
            start_bn: "20305518".to_string(),
            end_block: "20305518".to_string(),
            chain_id: "ETH".to_string(),
            address: "0xdAC17F958D2ee523a2206206994597C13D831ec7".to_string(),
        });

        let response = client.start_tx_query(request).await?;
        println!("RESPONSE={:?}", response.into_inner().message);

        // let end_request = tonic::Request::new(QueryEndRequest {
        //     activity_name: "Activity672".to_string(),
        //     end_block: "103".to_string(),
        // });
        // let end_response = client.end_query(end_request).await?;
        // println!("RESPONSE={:?}", end_response.into_inner().message);

        Ok(())
    }

    #[tokio::test]
    async fn test_high_concurrent_queries() -> Result<(), Box<dyn std::error::Error>> {
        let channel = Channel::from_static("http://45.77.43.70:50051")
            .connect()
            .await?;

        let client = QueryServiceClient::new(channel);

        let clients = std::iter::repeat_with(|| client.clone())
            .take(1000)
            .collect::<Vec<_>>();

        let tasks = clients.into_iter().enumerate().map(|(index, mut client)| {
            async move {
                let request = tonic::Request::new(QueryRequest {
                    activity_name: format!("Activity{}", index),
                    start_bn: format!("{}", 100 + index),
                    end_block: format!("{}", 105 + index),
                    chain_id: "ETH".to_string(),
                });

                let response = client.start_query(request).await?;
                println!("RESPONSE Task {}={:?}", index, response.into_inner().message);

                let end_request = tonic::Request::new(QueryEndRequest {
                    activity_name: format!("Activity{}", index),
                    end_block: format!("{}", 103 + index),  // Assuming an end block logic
                });

                let end_response = client.end_query(end_request).await?;
                println!("RESPONSE EndQuery Task {}={:?}", index, end_response.into_inner().message);
                Result::<(), tonic::Status>::Ok(())
            }
        }).collect::<Vec<_>>();

        let results = futures::future::join_all(tasks).await;

        results.into_iter().collect::<Result<Vec<_>, _>>()?;

        Ok(())
    }
}


pub struct MyQueryService;

#[tonic::async_trait]
impl QueryService for MyQueryService {
    async fn start_query(
        &self,
        request: tonic::Request<QueryRequest>,
    ) -> Result<tonic::Response<QueryResponse>, tonic::Status> {
        add_query_task(workflow::QueryRequest {
            activity_name: request.get_ref().activity_name.clone(),
            start_bn: request.get_ref().start_bn.clone(),
            end_block: request.get_ref().end_block.clone(),
            chain_id: request.get_ref().chain_id.clone(),
        }).await;
        Ok(tonic::Response::new(QueryResponse {
            message: "Query started".to_string(),
        }))
    }

    async fn end_query(
        &self,
        request: tonic::Request<QueryEndRequest>,
    ) -> Result<tonic::Response<QueryResponse>, tonic::Status> {
        end_task(workflow::QueryEndRequest {
            activity_name: request.get_ref().activity_name.clone(),
            end_block: request.get_ref().end_block.clone(),
        }).await;
        Ok(tonic::Response::new(QueryResponse {
            message: "Query ended".to_string(),
        }))
    }

    async fn start_tx_query(&self, request: Request<TxQueryRequest>)
                            -> Result<Response<QueryResponse>, Status> {
        let response_message = if should_start(&request.get_ref().activity_name).await {
            add_tx_query_task(workflow::TxQueryRequest {
                activity_name: request.get_ref().activity_name.clone(),
                start_bn: request.get_ref().start_bn.clone(),
                end_block: request.get_ref().end_block.clone(),
                chain_id: request.get_ref().chain_id.clone(),
                address: request.get_ref().address.clone(),
            }).await;
            StatusCode::Ok.as_str().to_string()
        } else {
            StatusCode::AlreadyStarted.as_str().to_string()
        };
        Ok(tonic::Response::new(QueryResponse {
            message: response_message,
        }))
    }
}

#[derive(Debug)]
enum StatusCode {
    Ok = 200,
    AlreadyStarted = 300,
    Error = 400,
}

impl StatusCode {
    fn as_str(&self) -> &'static str {
        match self {
            StatusCode::Ok => "200",
            StatusCode::AlreadyStarted => "300",
            StatusCode::Error => "400",
        }
    }
}

#[derive(Debug)]
pub struct ServerConfig {
    pub address: String,
}

impl ServerConfig {
    pub fn default() -> Self {
        Self {
            address: "127.0.0.1:50051".to_string(),
        }
    }
}

pub async fn init<T: QueryService + 'static>(service: T, config: ServerConfig) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let addr: SocketAddr = config.address.parse()?;
    info!("rpc start listen {:?}", config);
    Server::builder()
        .add_service(QueryServiceServer::new(service))
        .serve(addr)
        .await?;
    Ok(())
}
