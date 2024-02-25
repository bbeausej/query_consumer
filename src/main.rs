use clap::{Arg, ArgAction, Command};

use futures_util::TryStreamExt;
use log::{info, warn};
use simplelog::*;
use tokio::task::{JoinError, JoinHandle};

use core::borrow;
use std::error::Error;
use std::fs::File;
use std::sync::Mutex;

use polars::prelude::*;

use rdkafka::config::RDKafkaLogLevel;
use rdkafka::consumer::{Consumer, ConsumerContext, Rebalance, StreamConsumer};
use rdkafka::error::KafkaResult;
use rdkafka::message::{BorrowedMessage, Message, OwnedMessage};
use rdkafka::util::get_rdkafka_version;
use rdkafka::{ClientContext, TopicPartitionList};

use serde::{Deserialize, Serialize};

#[derive(Debug)]
enum ConsumerError {
    Async(JoinError),
    DataFrame(PolarsError),
    Deserialize(serde_json::Error),
    InvalidMessage(),
    Lock(),
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct KafkaQueryBody {
    time: i64,
    query: String,
    bind_vars: serde_json::value::Value,
}
#[derive(Debug, Serialize, Deserialize)]
struct KafkaMessage {
    name: String,
    body: KafkaQueryBody,
}

struct CliArguments {
    topics: Vec<String>,
    brokers: String,
    consumer_group_id: String,
}

const MESSAGE_TRESHOLD: i32 = 1000;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    CombinedLogger::init(vec![TermLogger::new(
        LevelFilter::Info,
        Config::default(),
        TerminalMode::Mixed,
        ColorChoice::Auto,
    )])
    .unwrap();

    info!("startup");

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

    info!("Starting consumer");
    consume(cli_args).await
}

struct CustomContext;
impl ClientContext for CustomContext {}
impl ConsumerContext for CustomContext {
    fn pre_rebalance(&self, rebalance: &Rebalance) {
        info!("Pre rebalance {:?}", rebalance);
    }

    fn post_rebalance(&self, rebalance: &Rebalance) {
        info!("Post rebalance {:?}", rebalance);
    }

    fn commit_callback(&self, result: KafkaResult<()>, _offsets: &TopicPartitionList) {
        info!("Committing offsets: {:?}", result);
    }
}

type LoggingConsumer = StreamConsumer<CustomContext>;

fn parse_message(msg: &OwnedMessage) -> Result<KafkaMessage, ConsumerError> {
    let payload = match msg.payload_view::<str>() {
        None => "",
        Some(Ok(s)) => s,
        Some(Err(e)) => {
            warn!("Error while deserializing message payload: {:?}", e);
            ""
        }
    };

    let kafka_message: KafkaMessage =
        serde_json::from_str(payload).map_err(ConsumerError::Deserialize)?;

    Ok(kafka_message)
}

fn stream_reader<'a>(
    data_buffer: &Arc<Mutex<DataFrame>>,
    borrowed_message: BorrowedMessage<'a>,
) -> JoinHandle<Result<(), ConsumerError>> {
    let owned_message = borrowed_message.detach();
    let buffer = data_buffer.clone();

    tokio::spawn(async move {
        info!(
            "Incoming message from topic: {} partition: {} offset: {}",
            owned_message.topic(),
            owned_message.partition(),
            owned_message.offset()
        );

        let incoming_message = tokio::task::spawn_blocking(move || parse_message(&owned_message))
            .await
            .map_err(ConsumerError::Async)?;

        if let Ok(message) = incoming_message {
            info!("Message: {:?}", message.body.query);

            let extension = DataFrame::new(vec![
                Series::new("timestamp", &[message.body.time]),
                Series::new("query", &[message.body.query]),
                Series::new("bind_vars", &[message.body.bind_vars.to_string()]),
            ])
            .map_err(ConsumerError::DataFrame)?;

            buffer
                .lock()
                .map_err(|_| ConsumerError::Lock())?
                .vstack(&extension)
                .map_err(ConsumerError::DataFrame)?;

            return Ok(());
        }

        Err(ConsumerError::InvalidMessage())
    })
}

async fn consume(args: CliArguments) -> Result<(), Box<dyn Error>> {
    // Kafka consumer configuration
    let context = CustomContext;
    let consumer: LoggingConsumer = rdkafka::config::ClientConfig::new()
        .set("group.id", args.consumer_group_id)
        .set("bootstrap.servers", args.brokers)
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "6000")
        .set("enable.auto.commit", "true")
        .set("auto.offset.reset", "beginning")
        .set_log_level(RDKafkaLogLevel::Debug)
        .create_with_context(context)
        .expect("Consumer creation failed");

    let topics: Vec<&str> = args.topics.iter().map(AsRef::as_ref).collect();

    // Subscribe to Kafka topic
    consumer
        .subscribe(&topics)
        .expect("failed to subscribe to topics");

    // let polars_schema = Schema::from_iter(
    //     vec![
    //         Field::new("timestamp", DataType::Int64),
    //         Field::new("query", DataType::String),
    //         Field::new("bind_vars", DataType::String),
    //     ]
    //     .into_iter(),
    // );

    let data_frame = DataFrame::new(vec![
        Series::new("timestamp", vec![0i64]),
        Series::new("query", &[String::from("")]),
        Series::new("bind_vars", &[String::from("")]),
    ])?;

    let data_buffer = Arc::new(Mutex::new(data_frame));

    let mut message_count = 0;
    let mut partition: i32 = 0;

    let stream_processor = consumer.stream().try_for_each(|borrowed_message| {
        let data_buffer = data_buffer.clone();
        async move {
            stream_reader(&data_buffer, borrowed_message).await.unwrap();
            Ok(())
        }
    });

    info!("Starting processing loop");
    stream_processor.await.expect("stream processing failed");
    info!("Stream processing terminated");
    Ok(())
}

// Consume loop
// let stream_processor = consumer.stream()(|msg| {
//     async move {
//         tokio::spawn(async move {

//         });
//     }
// });

// match consumer.recv().await {
//     Err(e) => error!("Error receiving message: {}", e),
//     Ok(msg) => {
//         info!(
//             "Incoming message from topic: {} partition: {} offset: {}",
//             msg.topic(),
//             msg.partition(),
//             msg.offset()
//         );

//         let payload = match msg.payload_view::<str>() {
//             None => "",
//             Some(Ok(s)) => s,
//             Some(Err(e)) => {
//                 warn!("Error while deserializing message payload: {:?}", e);
//                 ""
//             }
//         };

//         let kafka_message: KafkaMessage = serde_json::from_str(payload)?;
//         tokio::spawn(async move {
//             let kafka_message: KafkaMessage = serde_json::from_str(payload)?;

//             handler(socket, ld).await;
//         });

//         info!("Message: {:?}", kafka_message.body.query);

//         data_frame.extend(&DataFrame::new(vec![
//             Series::new("timestamp", &[kafka_message.body.time]),
//             Series::new("query", &[kafka_message.body.query]),
//             Series::new("bind_vars", &["".to_string()]),
//         ])?)?;

//         // data_frame.extend(&DataFrame::new(vec![
//         //     Series::new("timestamp", &[1 as i64]),
//         //     Series::new("query", &["thequery"]),
//         //     Series::new("bind_vars", &["thebinds"]),
//         // ])?)?;

//         message_count += 1;

//         if message_count >= MESSAGE_TRESHOLD {
//             partition += 1;
//             let file = File::create(format!("queries-{}.parquet", partition))?;
//             ParquetWriter::new(file).finish(&mut data_frame)?;
//             message_count = 0;
//         }

//         // consumer.commit_message(&msg, CommitMode::Async).unwrap();
//     }

// }
