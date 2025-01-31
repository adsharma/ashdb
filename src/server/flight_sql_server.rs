use arrow::{
    array::StringArray,
    datatypes::{DataType, Field, Schema},
    ipc,
    record_batch::RecordBatch,
};
use arrow_flight::{
    flight_service_server::{FlightService, FlightServiceServer},
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightEndpoint, FlightInfo,
    HandshakeRequest, HandshakeResponse, IpcMessage, PollInfo, PutResult, SchemaAsIpc,
    SchemaResult, Ticket,
};
use async_stream::stream;
use bytes::Bytes;
use futures::stream::BoxStream;
use futures::StreamExt;
use std::collections::HashMap;
use std::{net::SocketAddrV4, sync::Arc};
use tokio::sync::Mutex;
use tonic::{transport::Server, Request, Response, Status, Streaming};

use arrow::ipc::writer::IpcWriteOptions;
use arrow::ipc::writer::StreamWriter;

struct FlightServiceImpl {
    kv_store: Arc<Mutex<HashMap<String, Bytes>>>,
    server_location: arrow_flight::Location, // Store the server's location here
}

fn record_batch_to_flight_data(
    record_batch: RecordBatch,
) -> Result<FlightData, Box<dyn std::error::Error>> {
    // Create an IPC writer
    let options = IpcWriteOptions::default();
    let mut writer =
        StreamWriter::try_new_with_options(Vec::new(), &record_batch.schema(), options)?;

    // Write the RecordBatch to the IPC stream
    writer.write(&record_batch)?;
    writer.finish()?;

    // Get the serialized data
    let serialized_data = writer.into_inner()?;

    // Create FlightData
    let flight_data = FlightData {
        data_header: Bytes::from(record_batch.schema().as_ref().to_string()), // Serialize schema
        data_body: Bytes::from(serialized_data), // Serialized RecordBatch data
        ..Default::default()
    };

    Ok(flight_data)
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
        // Define the schema for the key-value table
        let schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("value", DataType::Utf8, false),
        ]));

        // Lock the kv_store to access the data
        let mut kv_store = self.kv_store.lock().await;

        kv_store.insert("key1".to_string(), Bytes::from("value1"));
        kv_store.insert("key2".to_string(), Bytes::from("value2"));

        // Calculate total_records (number of keys in the HashMap)
        let total_records = kv_store.len() as i64;

        // Calculate total_bytes (sum of the lengths of all Bytes values)
        let total_bytes: i64 = kv_store.values().map(|bytes| bytes.len() as i64).sum();

        // Create a ticket for querying the kvstore
        let ticket = Ticket {
            ticket: Bytes::from_static(b"kvstore"), // Identifier for the kvstore
        };

        // Define the endpoint for the kvstore
        let endpoint = FlightEndpoint {
            ticket: Some(ticket),
            location: vec![self.server_location.clone()], // Add locations if needed
            expiration_time: None,
            app_metadata: Bytes::new(),
        };

        // Create a FlightInfo object describing the table
        let flight_info = FlightInfo::new()
            .try_with_schema(&schema)
            .expect("Encoding failed")
            .with_endpoint(endpoint)
            .with_descriptor(_request.into_inner())
            .with_total_records(total_records)
            .with_total_bytes(total_bytes);

        Ok(Response::new(flight_info))
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
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        let ticket = request.into_inner();

        // Extract the key from the ticket
        let key = String::from_utf8(ticket.ticket.to_vec())
            .map_err(|e| Status::invalid_argument(e.to_string()))?;

        // Fetch the value from the key-value store
        let kv_store = self.kv_store.lock().await;
        let value = kv_store.get(&key).cloned().unwrap_or_default();

        // Create a RecordBatch with the key and value
        let schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("value", DataType::Utf8, false),
        ]));

        let key_array = StringArray::from(vec![key]);
        let value_array =
            StringArray::from(vec![String::from_utf8(value.to_vec()).unwrap_or_default()]);

        let record_batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(key_array), Arc::new(value_array)],
        )
        .map_err(|e| Status::internal(e.to_string()))?;

        // Serialize the RecordBatch into FlightData
        let flight_data = record_batch_to_flight_data(record_batch)
            .map_err(|e| Status::internal(e.to_string()))?;

        // Create a stream with the FlightData
        let stream = stream! {
            yield Ok(flight_data);
        };
        Ok(Response::new(Box::pin(stream)))
    }

    async fn do_put(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        let mut stream = _request.into_inner();
        let kv_store = self.kv_store.clone();

        let response_stream = async_stream::stream! {
            let mut processed_count = 0;
            let mut error_count = 0;

            while let Some(flight_data) = stream.next().await {
                match flight_data {
                    Ok(data) => {
                        // Parse the FlightData into a RecordBatch
                        let mut reader = arrow::ipc::reader::StreamReader::try_new(data.data_body.as_ref(), None)
                            .map_err(|e| Status::internal(e.to_string()))?;
                        let record_batch = reader.next()
                            .ok_or_else(|| Status::internal("No record batch found"))?
                            .map_err(|e| Status::internal(e.to_string()))?;

                        // Extract key and value from the RecordBatch
                        let key_array = record_batch.column(0).as_any().downcast_ref::<StringArray>().unwrap();
                        let value_array = record_batch.column(1).as_any().downcast_ref::<StringArray>().unwrap();

                        let key = key_array.value(0).to_string();
                        let value = Bytes::from(value_array.value(0).to_string());

                        // Store the key-value pair
                        let mut kv_store = kv_store.lock().await;
                        kv_store.insert(key, value);
                        processed_count += 1;
                    }
                    Err(e) => {
                        error_count += 1;
                        yield Err(e);
                    }
                }
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
    let service = FlightServiceImpl {
        kv_store: Arc::new(Mutex::new(HashMap::new())),
        server_location: arrow_flight::Location {
            uri: format!("grpc://{}", addr).into(),
        },
    };

    let svc = FlightServiceServer::new(service);

    Server::builder()
        .add_service(svc)
        .serve(addr.into())
        .await?;
    Ok(())
}
