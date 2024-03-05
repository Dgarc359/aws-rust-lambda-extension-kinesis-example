use aws_sdk_kinesis as kinesis;
use aws_sdk_kinesis::{Client, types::PutRecordsRequestEntry, primitives::Blob};
use lambda_extension::{tracing, Error, Extension, LambdaLog, LambdaLogRecord, Service, SharedService};
use std::{future::Future,pin::Pin, task::Poll};


#[derive(Clone)]
struct KinesisLogsProcessor {
    client: Client,
}

impl KinesisLogsProcessor {
    pub fn new(client: Client) -> Self {
        KinesisLogsProcessor { client }
    }
}

impl Service<Vec<LambdaLog>> for KinesisLogsProcessor {
    type Response = ();
    type Error = Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut core::task::Context<'_>) -> core::task::Poll<Result<(),Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, logs: Vec<LambdaLog>) -> Self::Future {
        let mut records = Vec::with_capacity(logs.len());

        for log in logs {
            match log.record {
                LambdaLogRecord::Function(record) => {
                    // push to kinesis
                    let pr_request_entry = PutRecordsRequestEntry::builder()
                        .data(Blob::new(record.as_bytes()))
                        .build()
                        .expect("Error building put records entry for KDS");
                    records.push(pr_request_entry)
                }
                _ => unreachable!(),
            }
        }

        let fut = self
            .client
            .put_records()
            .set_records(Some(records))
            .stream_name(std::env::var("KDS_NAME").unwrap())
            .send();

        Box::pin(async move {
            let _ = fut.await?;
            Ok(())
        })
    }
}


#[tokio::main]
async fn main() -> Result<(), kinesis::Error> {
    tracing::init_default_subscriber();

    let config = aws_config::load_from_env().await;
    let logs_processor = SharedService::new(
        KinesisLogsProcessor::new(
            Client::new(&config)));

    Extension::new()
        .with_log_types(&["function"])
        .with_logs_processor(logs_processor)
        .run()
        .await
        .unwrap();

    Ok(())
}
