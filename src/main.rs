pub mod consumer;
pub mod errors;

use consumer::LoggingConsumer;
use errors::{ConsumerError, ConsumerResult};

use clap::{Arg, ArgAction, Command};

use std::error::Error;
use std::fs::File;
use std::sync::atomic::{self, AtomicUsize};

use log::{info, warn};
use simplelog::*;

use futures_util::TryStreamExt;

use tokio::signal;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use polars::prelude::*;

use rdkafka::config::RDKafkaLogLevel;
use rdkafka::consumer::Consumer;
use rdkafka::message::{BorrowedMessage, Message, OwnedMessage};
use rdkafka::util::get_rdkafka_version;

use serde::{Deserialize, Serialize};

use crate::consumer::CustomConsumerContext;

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

#[allow(dead_code)]
enum TaskCommand {
    Message(KafkaMessage),
    Flush(),
    Shutdown(),
}

struct CliArguments {
    topics: Vec<String>,
    brokers: String,
    consumer_group_id: String,
    write_treshold: usize,
    debug: bool,
}

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
    msg_count: Arc<AtomicUsize>,
    tx: Sender<TaskCommand>,
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
            if !tx.is_closed() {
                tx.send(TaskCommand::Message(message))
                    .await
                    .map_err(|_| ConsumerError::Shutdown())?;
            }
            msg_count.fetch_add(1, atomic::Ordering::Relaxed);
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

fn write_dataframe(
    filename: &str,
    timestamps: &Vec<i64>,
    queries: &Vec<String>,
    bind_vars: &Vec<String>,
) -> ConsumerResult<()> {
    let len = timestamps.len();
    let start_time = std::time::Instant::now();
    info!("Flushing {} records dataframe to disk: {}", len, filename);

    let mut data_frame = df!(
        "timestamp" => &timestamps,
        "query" => &queries,
        "bind_vars" => &bind_vars,
    )?
    .sort(["timestamp"], true, false)?;

    let file = File::create(filename)?;
    ParquetWriter::new(file).finish(&mut data_frame)?;

    info!(
        "Flushing {} records dataframe took {:?}",
        len,
        start_time.elapsed()
    );

    Ok(())
}

async fn sink_task(
    msg_count: Arc<AtomicUsize>,
    cancel_token: CancellationToken,
    treshold: usize,
    mut rx: Receiver<TaskCommand>,
) -> () {
    let mut flushes: usize = 0;
    let mut timestamps: Vec<i64> = Vec::new();
    let mut queries: Vec<String> = Vec::new();
    let mut bind_vars: Vec<String> = Vec::new();

    let mut shutdown = false;
    let mut forced_flush = false;

    //while let Some(command) = rx.recv().await {
    loop {
        tokio::select! {
            _ = cancel_token.cancelled() => {
                info!("Sync task cancelling...");
                shutdown = true;
                forced_flush = true;
            }
            Some(command) = rx.recv() => {
                match command {
                    TaskCommand::Message(message) => {
                        timestamps.push(message.body.time);
                        queries.push(message.body.query);
                        bind_vars.push(message.body.bind_vars.to_string());
                        debug!("Sink buffer {} / {}", timestamps.len(), msg_count.load(atomic::Ordering::Relaxed));
                    }
                    TaskCommand::Flush() => {
                        forced_flush = true;
                    }
                    TaskCommand::Shutdown() => {
                        info!("Received sync shutdown");
                        forced_flush = true;
                        shutdown = true;
                    }
                }
            }
        }

        let should_flush = timestamps.len() >= treshold || shutdown;
        if should_flush || forced_flush {
            let filename = format!("queries-{}.parquet", flushes + 1);
            let written = write_dataframe(&filename, &timestamps, &queries, &bind_vars);
            if let Err(e) = written {
                error!("Failed to write file: {:?}", e);
                continue;
            }

            flushes += 1;

            timestamps.clear();
            queries.clear();
            bind_vars.clear();
        }

        if shutdown {
            info!("Terminating sync task...");
            return;
        }
    }
}

async fn consume(
    cancel_token: CancellationToken,
    args: CliArguments,
) -> Result<(), Box<dyn Error>> {
    // Kafka consumer configuration
    let context = CustomConsumerContext;
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

    let (tx, rx) = tokio::sync::mpsc::channel::<TaskCommand>(1000);

    let treshold = args.write_treshold;
    let atomic_msg_count = Arc::new(AtomicUsize::new(0));

    let sink_task = tokio::task::spawn(sink_task(
        atomic_msg_count.clone(),
        cancel_token.clone(),
        treshold,
        rx,
    ));

    let consume_task = consumer.stream().try_for_each(|borrowed_message| {
        let tx = tx.clone();
        let cancel_token = cancel_token.clone();
        let atomic_msg_count = atomic_msg_count.clone();
        async move {
            if !cancel_token.is_cancelled() {
                message_processor(atomic_msg_count, tx, borrowed_message);
            }
            Ok(())
        }
    });

    info!("Starting consumption loop");

    tokio::select! {
        _ = consume_task => {
            info!("Consume task exited.");
        }
        _  = sink_task => {
            info!("Sink task exited.");
        }
    }

    Ok(())
}

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
        .arg(
            Arg::new("write-treshold")
                .short('w')
                .long("write-treshold")
                .help("Write treshold")
                .value_parser(clap::value_parser!(usize))
                .default_value("10000"),
        )
        .arg(
            Arg::new("debug")
                .short('d')
                .long("debug")
                .action(ArgAction::SetTrue)
                .help("Debug logging"),
        )
        .get_matches();

    let cli_args = CliArguments {
        topics: matches.remove_many("topics").unwrap().collect(),
        brokers: matches.remove_one::<String>("brokers").unwrap(),
        consumer_group_id: matches.remove_one::<String>("consumer-group-id").unwrap(),
        write_treshold: matches.remove_one::<usize>("write-treshold").unwrap(),
        debug: matches.remove_one::<bool>("debug").unwrap(),
    };

    let log_level = if cli_args.debug {
        LevelFilter::Debug
    } else {
        LevelFilter::Info
    };

    CombinedLogger::init(vec![TermLogger::new(
        log_level,
        Config::default(),
        TerminalMode::Mixed,
        ColorChoice::Auto,
    )])
    .unwrap();

    info!("query_consumer v{}", env!("CARGO_PKG_VERSION"));

    let (version_n, version_s) = get_rdkafka_version();
    info!("rd_kafka_version: 0x{:08x}, {}", version_n, version_s);

    info!("Starting consumer");

    let cancel_token = CancellationToken::new();
    let child_token = cancel_token.child_token();

    tokio::spawn(async move {
        signal::ctrl_c().await.unwrap();
        info!("CTRL-C received...terminating tasks");
        cancel_token.cancel();
    });

    consume(child_token, cli_args).await?;

    info!("Exiting.");

    Ok(())
}
