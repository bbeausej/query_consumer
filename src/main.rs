pub mod consumer;
pub mod errors;

use consumer::LoggingConsumer;
use errors::{ConsumerError, ConsumerResult};

use clap::{Arg, ArgAction, Command};
use futures::io::sink;
use futures::stream::FuturesUnordered;
use futures::{Future, StreamExt};

use std::error::Error;
use std::fs::File;
use std::sync::atomic::{self, AtomicUsize};

use log::{info, warn};
use simplelog::*;

use tokio::runtime::Handle;
use tokio::signal;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use polars::prelude::*;

use rdkafka::config::RDKafkaLogLevel;
use rdkafka::consumer::Consumer;
use rdkafka::message::{Message, OwnedMessage};
use rdkafka::util::get_rdkafka_version;

use serde::{Deserialize, Serialize};
use serde_json::value::Value;

use crate::consumer::CustomConsumerContext;

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct KafkaQueryBody {
    time: i64,
    query: String,
    bind_vars: Value,
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
    concurrency: usize,
}

fn parse_message<'msg>(msg: &'msg OwnedMessage) -> ConsumerResult<KafkaMessage> {
    let start_time = std::time::Instant::now();

    let payload = match msg.payload_view::<str>() {
        None => "",
        Some(Ok(s)) => s,
        Some(Err(e)) => {
            warn!("Error while deserializing message payload: {:?}", e);
            ""
        }
    };

    let kafka_message: KafkaMessage = serde_json::from_str(payload)?;

    let elapsed = start_time.elapsed().as_millis();
    if elapsed > 10 {
        info!(
            "Deserialize time: {:?} len {:?}",
            start_time.elapsed(),
            payload.len()
        );
    }

    Ok(kafka_message)
}

// fn get_schema() -> Schema {
//     Schema::from_iter(vec![
//         Field::new("timestamp", DataType::Int64),
//         Field::new("query", DataType::String),
//         Field::new("bind_vars", DataType::String),
//     ])
// }

fn message_processor<'msg>(
    msg_count: Arc<AtomicUsize>,
    tx: Sender<TaskCommand>,
    owned_message: OwnedMessage,
) -> JoinHandle<ConsumerResult<()>> {
    tokio::spawn(async move {
        debug!(
            "Incoming message from topic: {} partition: {} offset: {}",
            owned_message.topic(),
            owned_message.partition(),
            owned_message.offset()
        );

        let incoming_message = parse_message(&owned_message);

        if let Ok(msg) = incoming_message {
            if !tx.is_closed() {
                tx.send(TaskCommand::Message(msg))
                    .await
                    .map_err(|_| ConsumerError::Shutdown())?;
            }
            msg_count.fetch_add(1, atomic::Ordering::Relaxed);
            return Ok(());
        }

        Err(ConsumerError::InvalidMessage())
    })
}

async fn report_task(
    read_msg_count: Arc<AtomicUsize>,
    sink_msg_count: Arc<AtomicUsize>,
) -> JoinHandle<()> {
    tokio::task::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(1));
        let mut read_last = 0;
        let mut sunk_last = 0;
        loop {
            interval.tick().await;

            let read_current = read_msg_count.load(atomic::Ordering::Relaxed);
            let sunk_current = sink_msg_count.load(atomic::Ordering::Relaxed);

            info!(
                "Stats: {} read ({}/s) {} ({}/s) sunk",
                read_current,
                (read_current - read_last),
                sunk_current,
                (sunk_current - sunk_last)
            );

            read_last = read_current;
            sunk_last = sunk_current;
        }
    })
}

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

fn sink_task(
    msg_count: Arc<AtomicUsize>,
    cancel_token: CancellationToken,
    treshold: usize,
    mut rx: Receiver<TaskCommand>,
) -> () {
    let mut flushes: usize = 0;
    let mut timestamps: Vec<i64> = Vec::with_capacity(treshold);
    let mut queries: Vec<String> = Vec::with_capacity(treshold);
    let mut bind_vars: Vec<String> = Vec::with_capacity(treshold);

    let mut shutdown = false;

    loop {
        let mut forced_flush = false;

        if cancel_token.is_cancelled() {
            shutdown = true;
            forced_flush = true;
        }

        if let Some(command) = rx.blocking_recv() {
            match command {
                TaskCommand::Message(message) => {
                    timestamps.push(message.body.time);
                    queries.push(message.body.query);
                    // bind_vars.push(message.body.bind_vars.to_string());
                    msg_count.fetch_add(1, atomic::Ordering::Relaxed);
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

    let (tx, rx) = tokio::sync::mpsc::channel(1000);

    let treshold = args.write_treshold.clone();

    let read_msg_count = Arc::new(AtomicUsize::new(0));
    let sink_msg_count = Arc::new(AtomicUsize::new(0));

    tokio::spawn(report_task(
        Arc::clone(&read_msg_count),
        Arc::clone(&sink_msg_count),
    ));

    let sink_cancel_token = cancel_token.clone();
    let sink_task = tokio::task::spawn_blocking(move || {
        sink_task(sink_msg_count, sink_cancel_token, treshold, rx)
    });

    let consume_task = tokio::task::spawn_blocking(move || {
        Handle::current().block_on(async move {
            let mut futures: FuturesUnordered<_> = FuturesUnordered::new();
            while let Ok(borrowed_message) = consumer.stream().next().await.unwrap() {
                if futures.len() > args.concurrency {
                    futures.next().await;
                }

                let tx = tx.clone();
                let cancel_token = cancel_token.clone();
                let read_msg_count = read_msg_count.clone();

                futures.push(message_processor(
                    read_msg_count,
                    tx,
                    borrowed_message.detach(),
                ));

                while let Some(_) = futures.next().await {}
            }
        });
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
        .arg(
            Arg::new("concurrency")
                .short('c')
                .long("concurrency")
                .value_parser(clap::value_parser!(usize))
                .help("Read messages concurrency")
                .default_value("100"),
        )
        .get_matches();

    let cli_args = CliArguments {
        topics: matches.remove_many("topics").unwrap().collect(),
        brokers: matches.remove_one::<String>("brokers").unwrap(),
        consumer_group_id: matches.remove_one::<String>("consumer-group-id").unwrap(),
        write_treshold: matches.remove_one::<usize>("write-treshold").unwrap(),
        debug: matches.remove_one::<bool>("debug").unwrap(),
        concurrency: matches.remove_one::<usize>("concurrency").unwrap(),
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
