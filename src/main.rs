pub mod consumer;
pub mod errors;
use crate::consumer::CustomConsumerContext;

use consumer::LoggingConsumer;
use errors::{ConsumerError, ConsumerResult};

use clap::{Arg, ArgAction, Command};
use polars::io::parquet::BatchedWriter;

use std::error::Error;
use std::fs::{create_dir_all, File};
use std::path::Path;
use std::sync::atomic::{self, AtomicUsize};

use log::{info, warn};
use simplelog::*;

use tokio::runtime::Handle;
use tokio::signal;
use tokio::sync::mpsc::Receiver;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use polars::prelude::*;

use rdkafka::config::RDKafkaLogLevel;
use rdkafka::consumer::Consumer;
use rdkafka::message::{BorrowedMessage, Message};
use rdkafka::util::get_rdkafka_version;

use serde::{Deserialize, Serialize};

use humansize::{format_size, BINARY};

#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct KafkaQueryBody {
    time: i64,
    query: String,
    bind_vars: serde_json::value::Value,
    database: Option<String>,
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
    outpath: String,
    topics: Vec<String>,
    brokers: String,
    consumer_group_id: String,
    flush_treshold: usize,
    rotate_treshold: usize,
    debug: bool,
    workers: usize,
}

fn parse_message(msg: &BorrowedMessage) -> ConsumerResult<KafkaMessage> {
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
        debug!(
            "Deserialize time: {:?} len {:?}",
            start_time.elapsed(),
            format_size(payload.len(), BINARY),
        );
    }

    Ok(kafka_message)
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

fn get_dataframe_schema() -> Schema {
    Schema::from_iter(vec![
        Field::new("timestamp", DataType::Int64),
        Field::new("query", DataType::String),
        Field::new("bind_vars", DataType::String),
        Field::new("database", DataType::String),
    ])
}

fn open_dataframe_file(outpath: &str, filename: &str) -> ConsumerResult<BatchedWriter<File>> {
    let path = Path::new(outpath);
    create_dir_all(path)?;

    let file = File::create(path.join(filename))?;
    let mut file_writer = ParquetWriter::new(file);
    file_writer = file_writer.set_parallel(true);
    let batched = file_writer.batched(&get_dataframe_schema())?;
    Ok(batched)
}

fn write_dataframe(
    worker_id: usize,
    writer: &mut BatchedWriter<File>,
    timestamps: &Vec<i64>,
    queries: &Vec<String>,
    bind_vars: &Vec<String>,
    databases: &Vec<String>,
) -> ConsumerResult<()> {
    let len = timestamps.len();
    let start_time = std::time::Instant::now();
    info!(
        "[worker {}] Flushing {} records dataframe to disk",
        worker_id, len
    );

    let data_frame = df!(
        "timestamp" => &timestamps,
        "query" => &queries,
        "bind_vars" => &bind_vars,
        "database" => &databases,
    )?
    .sort(["timestamp"], true, false)?;

    writer.write_batch(&data_frame)?;

    info!(
        "[worker {}] Flushing {} records dataframe took {:?}",
        worker_id,
        len,
        start_time.elapsed()
    );

    Ok(())
}

fn sink_task(
    worker_id: usize,
    msg_count: Arc<AtomicUsize>,
    cancel_token: CancellationToken,
    outpath: &str,
    flush_treshold: usize,
    rotate_treshold: usize,
    mut rx: Receiver<TaskCommand>,
) {
    let mut rotations: usize = 0;
    let mut flushed: usize = 0;
    let mut timestamps: Vec<i64> = Vec::with_capacity(flush_treshold);
    let mut queries: Vec<String> = Vec::with_capacity(flush_treshold);
    let mut bind_vars: Vec<String> = Vec::with_capacity(flush_treshold);
    let mut databases: Vec<String> = Vec::with_capacity(flush_treshold);

    let mut shutdown = false;

    let filename = format!("queries-{}-{}.parquet", worker_id, rotations + 1);
    let mut writer: BatchedWriter<File> = open_dataframe_file(outpath, &filename).unwrap();

    loop {
        let mut forced_flush = false;

        if cancel_token.is_cancelled() {
            info!("[worker {}] Sync task was cancelled...", worker_id);
            shutdown = true;
            forced_flush = true;
        }

        if let Some(command) = rx.blocking_recv() {
            match command {
                TaskCommand::Message(message) => {
                    timestamps.push(message.body.time);
                    queries.push(message.body.query);
                    bind_vars.push(message.body.bind_vars.to_string());
                    if let Some(db) = message.body.database {
                        databases.push(db);
                    } else {
                        databases.push("".to_string());
                    }
                    msg_count.fetch_add(1, atomic::Ordering::Relaxed);
                }
                TaskCommand::Flush() => {
                    forced_flush = true;
                }
                TaskCommand::Shutdown() => {
                    info!("[worker {}] Received sync task shutdown", worker_id);
                    forced_flush = true;
                    shutdown = true;
                }
            }
        }

        let should_flush = timestamps.len() >= flush_treshold || shutdown;
        if should_flush || forced_flush {
            write_dataframe(
                worker_id,
                &mut writer,
                &timestamps,
                &queries,
                &bind_vars,
                &databases,
            )
            .unwrap();

            flushed += timestamps.len();

            timestamps.clear();
            queries.clear();
            bind_vars.clear();
            databases.clear();
        }

        let should_rotate = flushed >= rotate_treshold;
        if should_rotate {
            info!("[worker {}] Rotating output file", worker_id);
            writer.finish().unwrap();

            flushed = 0;
            rotations += 1;

            writer = open_dataframe_file(
                outpath,
                format!("queries-{}-{}.parquet", worker_id, rotations + 1).as_str(),
            )
            .unwrap();
        }

        if shutdown {
            info!("[worker {}] Terminating worker sync task...", worker_id);
            writer.finish().unwrap();
            return;
        }
    }
}

async fn run_worker(
    worker_id: usize,
    cancel_token: CancellationToken,
    args: Arc<CliArguments>,
) -> ConsumerResult<()> {
    // Kafka consumer configuration
    let context = CustomConsumerContext;
    let consumer: LoggingConsumer = rdkafka::config::ClientConfig::new()
        .set("group.id", &args.consumer_group_id)
        .set("bootstrap.servers", &args.brokers)
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
        .unwrap_or_else(|_| panic!("[worker {}] failed to subscribe to topics", worker_id));

    let (tx, rx) = tokio::sync::mpsc::channel(1000);

    let flush_treshold = args.flush_treshold;
    let rotate_treshold = args.rotate_treshold;
    let outpath = args.outpath.as_str();

    let read_msg_count = Arc::new(AtomicUsize::new(0));
    let sink_msg_count = Arc::new(AtomicUsize::new(0));

    tokio::task::spawn(report_task(
        Arc::clone(&read_msg_count),
        Arc::clone(&sink_msg_count),
    ));

    let runtime_handle = Handle::current();

    std::thread::scope(move |scope| {
        let sink_cancel_token = cancel_token.clone();
        scope.spawn(move || {
            info!("[worker {}] Starting sink thread", worker_id);
            sink_task(
                worker_id,
                sink_msg_count,
                sink_cancel_token,
                outpath,
                flush_treshold,
                rotate_treshold,
                rx,
            );
        });

        let consume_cancel_token = cancel_token.clone();
        scope.spawn(move || {
            info!("[worker {}] Starting consumption thread", worker_id);

            runtime_handle.block_on(async move {
                loop {
                    let incoming = tokio::select! {
                        _ = consume_cancel_token.cancelled() => {
                            info!("[worker {}] Consume task was cancelled...", worker_id);
                            break;
                        }
                        msg = consumer.recv() => {
                            msg
                        }
                    };

                    match incoming {
                        Err(e) => warn!("[worker {}] Kafka error: {}", worker_id, e),
                        Ok(msg) => {
                            let kmsg = parse_message(&msg);

                            if let Ok(msg) = kmsg {
                                read_msg_count.fetch_add(1, atomic::Ordering::Relaxed);

                                if !tx.is_closed() {
                                    tx.send(TaskCommand::Message(msg)).await.ok();
                                }
                            } else {
                                error!(
                                    "Failed to deserialize message {}-{}",
                                    msg.partition(),
                                    msg.offset()
                                );
                            }
                        }
                    }
                }
            })
        });
    });

    Ok(())
}

fn parse_cli_args() -> CliArguments {
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
            Arg::new("flush-treshold")
                .long("flush-treshold")
                .help("Messages accumulated before flushing to disk")
                .value_parser(clap::value_parser!(usize))
                .default_value("10000"),
        )
        .arg(
            Arg::new("rotate-treshold")
                .long("rotate-treshold")
                .help("Messages per file")
                .value_parser(clap::value_parser!(usize))
                .default_value("100000"),
        )
        .arg(
            Arg::new("debug")
                .short('d')
                .long("debug")
                .action(ArgAction::SetTrue)
                .help("Debug logging"),
        )
        .arg(
            Arg::new("workers")
                .short('w')
                .long("workers")
                .value_parser(clap::value_parser!(usize))
                .help("Numbers of workers")
                .default_value("1"),
        )
        .arg(
            Arg::new("outpath")
                .short('o')
                .long("outpath")
                .help("Output path")
                .default_value("./"),
        )
        .get_matches();

    CliArguments {
        topics: matches.remove_many("topics").unwrap().collect(),
        brokers: matches.remove_one::<String>("brokers").unwrap(),
        consumer_group_id: matches.remove_one::<String>("consumer-group-id").unwrap(),
        flush_treshold: matches.remove_one::<usize>("flush-treshold").unwrap(),
        rotate_treshold: matches.remove_one::<usize>("rotate-treshold").unwrap(),
        debug: matches.remove_one::<bool>("debug").unwrap(),
        workers: matches.remove_one::<usize>("workers").unwrap(),
        outpath: matches.remove_one::<String>("outpath").unwrap(),
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let cli_args = parse_cli_args();

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

    let cancel_token = CancellationToken::new();
    let child_token = cancel_token.child_token();

    tokio::spawn(async move {
        signal::ctrl_c().await.unwrap();
        info!("CTRL-C received...terminating tasks");
        cancel_token.cancel();
    });

    let num_workers = cli_args.workers;

    info!("Starting {} workers...", num_workers);

    let handle = Arc::new(Handle::current());
    let args = Arc::new(cli_args);

    std::thread::scope(|scope| {
        (0..num_workers)
            .map(|worker_id| {
                info!("Starting worker {}", worker_id);
                let child_token = child_token.clone();
                let args = Arc::clone(&args);
                let handle = Arc::clone(&handle);
                scope.spawn(move || {
                    handle.block_on(run_worker(worker_id, child_token, args))?;
                    Ok::<(), ConsumerError>(())
                })
            })
            .for_each(drop);
    });

    info!("Exiting.");

    Ok(())
}
