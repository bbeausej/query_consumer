pub mod errors;

use errors::{ConsumerError, ConsumerResult};

use clap::{Arg, ArgAction, Command};

use std::error::Error;
use std::fs::File;
// use std::iter::FromIterator;

use log::{info, warn};
use simplelog::*;

use futures_util::TryStreamExt;

use tokio::sync::mpsc::{Receiver, Sender};
use tokio::task::JoinHandle;

use polars::prelude::*;

use rdkafka::config::RDKafkaLogLevel;
use rdkafka::consumer::{Consumer, ConsumerContext, Rebalance, StreamConsumer};
use rdkafka::error::KafkaResult;
use rdkafka::message::{BorrowedMessage, Message, OwnedMessage};
use rdkafka::util::get_rdkafka_version;
use rdkafka::{ClientContext, TopicPartitionList};

use serde::{Deserialize, Serialize};

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
    write_treshold: usize,
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

fn parse_message(msg: &OwnedMessage) -> ConsumerResult<KafkaMessage> {
    let payload = match msg.payload_view::<str>() {
        None => "",
        Some(Ok(s)) => s,
        Some(Err(e)) => {
            warn!("Error while deserializing message payload: {:?}", e);
            ""
        }
    };

    let kafka_message: KafkaMessage = serde_json::from_str(payload)?;

    Ok(kafka_message)
}

// fn get_schema() -> Schema {
//     Schema::from_iter(vec![
//         Field::new("timestamp", DataType::Int64),
//         Field::new("query", DataType::String),
//         Field::new("bind_vars", DataType::String),
//     ])
// }

fn message_processor<'a>(
    tx: Sender<DataFrame>,
    borrowed_message: BorrowedMessage<'a>,
) -> JoinHandle<ConsumerResult<()>> {
    let owned_message = borrowed_message.detach();

    tokio::spawn(async move {
        debug!(
            "Incoming message from topic: {} partition: {} offset: {}",
            owned_message.topic(),
            owned_message.partition(),
            owned_message.offset()
        );

        let incoming_message =
            tokio::task::spawn_blocking(move || parse_message(&owned_message)).await?;

        if let Ok(message) = incoming_message {
            debug!("Message: {:?}", message.body.query);

            let extension = DataFrame::new(vec![
                Series::new("timestamp", &[message.body.time]),
                Series::new("query", &[message.body.query]),
                Series::new("bind_vars", &[message.body.bind_vars.to_string()]),
            ])?;

            tx.send(extension).await.unwrap();

            return Ok(());
        }

        Err(ConsumerError::InvalidMessage())
    })
}

// fn flush_task(data_buffer: &Arc<Mutex<DataFrame>>) -> JoinHandle<()> {
//     tokio::task::spawn(async move {
//         let mut flush_count = 0;
//         let mut interval = tokio::time::interval(std::time::Duration::from_millis(5000));
//         loop {
//             interval.tick().await;
//             flush_data(&flush_count, data_buffer.clone()).await.ok();
//             flush_count += 1;
//         }
//     })
// }

async fn sink_task(treshold: usize, mut rx: Receiver<DataFrame>, mut data_frame: DataFrame) -> () {
    let mut writes: usize = 0;

    while let Some(extension) = rx.recv().await {
        debug!("Received frame to process");

        data_frame = match data_frame.extend(&extension) {
            Ok(_) => data_frame,
            Err(e) => {
                error!("Error extending DataFrame: {:?}", e);
                data_frame
            }
        };

        debug!("DataFrame height {} / {}", data_frame.height(), treshold);

        if data_frame.height() >= treshold {
            writes += 1;

            let start_time = std::time::Instant::now();

            if data_frame.should_rechunk() {
                data_frame.align_chunks();
            }

            let filename = format!("queries-{}.parquet", writes);
            info!(
                "Flushing {} records dataframe to disk: {}",
                treshold, filename
            );

            let file = File::create(filename);
            if let Err(e) = file {
                error!("Failed to create file: {:?}", e);
                continue;
            }

            let written = ParquetWriter::new(file.unwrap()).finish(&mut data_frame);
            if let Err(e) = written {
                error!("Failed to write file: {:?}", e);
                continue;
            }

            data_frame = data_frame.clear();

            info!(
                "Flushing {} records dataframe took {:?}",
                treshold,
                start_time.elapsed()
            );
        }
    }
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

    let data_frame = DataFrame::new(vec![
        Series::new("timestamp", vec![0i64]),
        Series::new("query", &[String::from("")]),
        Series::new("bind_vars", &[String::from("")]),
    ])?;

    let (tx, rx) = tokio::sync::mpsc::channel::<DataFrame>(1000);

    let treshold = args.write_treshold;

    let sink_task = tokio::task::spawn(sink_task(treshold, rx, data_frame));

    let consume_task = consumer.stream().try_for_each(|borrowed_message| {
        let tx = tx.clone();
        async move {
            message_processor(tx, borrowed_message);
            Ok(())
        }
    });

    info!("Starting processing loop");
    consume_task.await.expect("stream processing failed");
    info!("Stream processing terminated");

    sink_task.abort();

    Ok(())
}

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
        .arg(
            Arg::new("write-treshold")
                .short('w')
                .long("write-treshold")
                .help("Write treshold")
                .value_parser(clap::value_parser!(usize))
                .default_value("10000"),
        )
        .get_matches();

    let (version_n, version_s) = get_rdkafka_version();
    info!("rd_kafka_version: 0x{:08x}, {}", version_n, version_s);

    let topics = matches.remove_many("topics").unwrap().collect();
    let brokers = matches.remove_one::<String>("brokers").unwrap();
    let consumer_group_id = matches.remove_one::<String>("consumer-group-id").unwrap();
    let write_treshold: usize = matches.remove_one::<usize>("write-treshold").unwrap();

    let cli_args = CliArguments {
        topics,
        brokers,
        consumer_group_id,
        write_treshold,
    };

    info!("Starting consumer");
    consume(cli_args).await
}
