use crate::query::query_service_server::{QueryService, QueryServiceServer};
use crate::query::{QueryEndRequest, QueryRequest, QueryResponse};
use tonic::transport::Server;
use std::net::SocketAddr;
use log::info;
use workflow::{add_query_task, end_task};

mod query;

#[cfg(test)]
mod tests {
    use tonic_build;
    use crate::{init, MyQueryService, ServerConfig};
    use tokio::time::{timeout, Duration};
    use tonic::transport::Channel;
    use crate::query::query_service_client::QueryServiceClient;
    use crate::query::{QueryEndRequest, QueryRequest};

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

        // let request = tonic::Request::new(QueryRequest {
        //     activity_name: "Activity2".to_string(),
        //     start_bn: "100".to_string(),
        //     end_block: "105".to_string(),
        //     chain_id: "ETH".to_string(),
        // });
        //
        // let response = client.start_query(request).await?;
        // println!("RESPONSE={:?}", response.into_inner().message);

        let end_request = tonic::Request::new(QueryEndRequest {
            activity_name: "Activity2".to_string(),
            end_block: "105".to_string(),
        });

        let end_response = client.end_query(end_request).await?;
        println!("RESPONSE={:?}", end_response.into_inner().message);

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
