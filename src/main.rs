use std::error::Error;
use std::fs::File;

use clap::{Arg, ArgAction, Command};
use log::{info, warn};

use polars::prelude::*;

use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::message::Message;
use rdkafka::util::get_rdkafka_version;

use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
struct KafkaMessage {
    timestamp: i64,
    query: String,
    bind_vars: String,
}

struct CliArguments {
    topics: Vec<String>,
    brokers: String,
    consumer_group_id: String,
}

const MESSAGE_TRESHOLD: i32 = 100;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let mut matches = Command::new("query-consumer")
        .version(option_env!("CARGO_PKG_VERSION").unwrap_or(""))
        .about("Simple command line consumer to parquet")
        .arg(
            Arg::new("brokers")
                .short('b')
                .long("brokers")
                .help("Broker list in kafka format")
                .default_value("localhost:9092"),
        )
        .arg(
            Arg::new("consumer-group-id")
                .short('g')
                .long("consumer-group-id")
                .help("Consumer group id")
                .default_value("query-consumer-id"),
        )
        .arg(
            Arg::new("log-conf")
                .long("log-conf")
                .help("Configure the logging format (example: 'rdkafka=trace')"),
        )
        .arg(
            Arg::new("topics")
                .short('t')
                .long("topics")
                .help("Topic list")
                .action(ArgAction::Append)
                .required(true),
        )
        .get_matches();

    let (version_n, version_s) = get_rdkafka_version();
    info!("rd_kafka_version: 0x{:08x}, {}", version_n, version_s);

    let topics = matches.remove_many("topics").unwrap().collect();
    let brokers = matches.remove_one::<String>("brokers").unwrap();
    let consumer_group_id = matches.remove_one::<String>("consumer-group-id").unwrap();

    let cli_args = CliArguments {
        topics,
        brokers,
        consumer_group_id,
    };

    consume(cli_args).await
}

async fn consume(args: CliArguments) -> Result<(), Box<dyn Error>> {
    // Kafka consumer configuration
    let consumer: StreamConsumer = rdkafka::config::ClientConfig::new()
        .set("group.id", args.consumer_group_id)
        .set("bootstrap.servers", args.brokers)
        .create()?;

    let topics: Vec<&str> = args.topics.iter().map(AsRef::as_ref).collect();

    // Subscribe to Kafka topic
    consumer.subscribe(&topics)?;

    // let polars_schema = Schema::from_iter(
    //     vec![
    //         Field::new("timestamp", DataType::Int64),
    //         Field::new("query", DataType::String),
    //         Field::new("bind_vars", DataType::String),
    //     ]
    //     .into_iter(),
    // );

    let mut data_frame = DataFrame::new(vec![
        Series::new("timestamp", vec![0i64]),
        Series::new("query", &[String::from("")]),
        Series::new("bind_vars", &[String::from("")]),
    ])?;

    let mut message_count = 0;
    let mut partition: i32 = 0;
    // Consume loop
    loop {
        let message = consumer.recv().await;
        match message {
            Ok(msg) => {
                let payload = match msg.payload_view::<str>() {
                    None => "",
                    Some(Ok(s)) => s,
                    Some(Err(e)) => {
                        warn!("Error while deserializing message payload: {:?}", e);
                        ""
                    }
                };

                let kafka_message: KafkaMessage = serde_json::from_str(payload)?;

                data_frame.vstack(&DataFrame::new(vec![
                    Series::new("timestamp", &[kafka_message.timestamp]),
                    Series::new("query", &[kafka_message.query]),
                    Series::new("bind_vars", &[kafka_message.bind_vars]),
                ])?)?;

                message_count += 1;

                if message_count >= MESSAGE_TRESHOLD {
                    partition += 1;
                    let file = File::create(format!("queries-{}.parquet", partition))?;
                    ParquetWriter::new(file).finish(&mut data_frame)?;
                    message_count = 0;
                }

                consumer.commit_message(&msg, CommitMode::Async).unwrap();
            }
            Err(e) => eprintln!("Error receiving message: {}", e),
        }
    }

    // Ok(())
}
