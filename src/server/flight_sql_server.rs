// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use futures::stream::BoxStream;
use futures::TryStreamExt;
use tonic::transport::Server;
use tonic::{Request, Response, Status, Streaming};
use std::net::{SocketAddrV4, Ipv4Addr};


use arrow_flight::{
    flight_service_server::FlightService, flight_service_server::FlightServiceServer, Action,
    ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo, HandshakeRequest,
    HandshakeResponse, PollInfo, PutResult, SchemaResult, Ticket,
};
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_array::record_batch;
use arrow_schema;
use arrow::record_batch::RecordBatch;

use async_stream::stream;
use arrow_flight::error::FlightError;

// slatedb
use bytes::Bytes;
use slatedb::config::DbOptions;
use slatedb::db::Db;
use slatedb::object_store::{local::LocalFileSystem, ObjectStore};
use std::sync::Arc;

#[derive(Clone)]
pub struct FlightServiceImpl {
    kv_store: Arc<Db>,
}

#[tonic::async_trait]
impl FlightService for FlightServiceImpl {
    type HandshakeStream = BoxStream<'static, Result<HandshakeResponse, Status>>;
    type ListFlightsStream = BoxStream<'static, Result<FlightInfo, Status>>;
    type DoGetStream = BoxStream<'static, Result<FlightData, Status>>;
    type DoPutStream = BoxStream<'static, Result<PutResult, Status>>;
    type DoActionStream = BoxStream<'static, Result<arrow_flight::Result, Status>>;
    type ListActionsStream = BoxStream<'static, Result<ActionType, Status>>;
    type DoExchangeStream = BoxStream<'static, Result<FlightData, Status>>;

    async fn handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        Err(Status::unimplemented("Implement handshake"))
    }

    async fn list_flights(
        &self,
        _request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        Err(Status::unimplemented("Implement list_flights"))
    }

    async fn get_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented("Implement get_flight_info"))
    }

    async fn poll_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<PollInfo>, Status> {
        Err(Status::unimplemented("Implement poll_flight_info"))
    }

    async fn get_schema(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        Err(Status::unimplemented("Implement get_schema"))
    }

    async fn do_get(
        &self,
        _request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        let descriptor = _request.into_inner();

        // Extract the key from the FlightDescriptor
        let key = descriptor.ticket.clone();

        // Get the value from the key-value store
        let value = match self.kv_store.get(&key).await {
            Ok(v) => if let Some(v1) = v { v1 } else {Bytes::from(vec![])},
            // TODO: better err handling
            Err(_) => Bytes::from(vec![])
        };
        // TODO: use non hard coded key/value here
        let item = record_batch!(("key", Int32, [1, 2, 3]), ("value", Utf8, ["a", "b", "c"]));
        let stream = async_stream::stream!{
            yield item.map_err(|e| FlightError::from(e))
        };
        let fd = FlightDataEncoderBuilder::new().build(stream).map_err(|e| Status::internal(e.to_string()));

        Ok(Response::new(Box::pin(fd)))
    }

    async fn do_put(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        // Create a stream for processing and responding
        let response_stream = async_stream::stream! {
                    let mut stream = _request.into_inner();
                    let mut processed_count = 0;
                    let mut error_count = 0;

                    while let Some(flight_data) = stream.try_next().await? {
                        // Assuming FlightData has key in headers and value in data_body
                        let key = flight_data.data_header;
                        let value = flight_data.data_body;
/*
                        match self.kv_store.put(&key, &value).await {
                            Ok(_) => processed_count += 1,
                            Err(_) => error_count += 1,
                        }
*/
                    }

                    // Yield a summary PutResult
                    yield Ok(PutResult {
                        app_metadata: format!(
                            "Processed: {}, Errors: {}",
                            processed_count,
                            error_count
                        ).into_bytes().into(),
                    });
                };

        Ok(Response::new(Box::pin(response_stream)))
    }

    async fn do_action(
        &self,
        _request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        Err(Status::unimplemented("Implement do_action"))
    }

    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        Err(Status::unimplemented("Implement list_actions"))
    }

    async fn do_exchange(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented("Implement do_exchange"))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr: SocketAddrV4 = "0.0.0.0:50051".parse()?;

    // Setup
    let object_store: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new());
    let options = DbOptions::default();
    let kv_store = Arc::new(Db::open_with_opts("/tmp/test_kv_store", options, object_store).await?);
    let service = FlightServiceImpl { kv_store };

    let svc = FlightServiceServer::new(service);

    Server::builder().add_service(svc).serve(addr.into()).await?;
    Ok(())
}
