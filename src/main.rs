extern crate clap;
extern crate env_logger;
#[macro_use]
extern crate log;
extern crate rdkafka;
extern crate uuid;
extern crate chrono;
#[macro_use] extern crate prettytable;
extern crate indicatif;

use clap::{App, Arg};
use std::collections::HashMap;
use metric::Metrics;
use std::time::Instant;
use prettytable::Table;
use prettytable::row::Row;
use prettytable::cell::Cell;

mod kafka;
mod metric;

fn main() {
    env_logger::init();

    let matches = App::new("Kafka Topic Analyzer")
        .bin_name("kafka-topic-analyzer")

        .arg(Arg::with_name("topic")
            .short("t")
            .long("topic")
            .value_name("TOPIC")
            .help("The topic to analyze")
            .takes_value(true)
            .required(true)
        )
        .arg(Arg::with_name("bootstrap-server")
            .short("b")
            .long("bootstrap-server")
            .value_name("BOOTSTRAP_SERVER")
            .help("Bootstrap server(s) to work with, comma separated")
            .takes_value(true)
            .required(true)
        )
        .get_matches();

    let start_time = Instant::now();

    let mut start_offsets = HashMap::<i32, i64>::new();
    let mut end_offsets = HashMap::<i32, i64>::new();
    let mut partitions = Vec::<i32>::new();
    let topic = matches.value_of("topic").unwrap();
    let bootstrap_server = matches.value_of("bootstrap-server").unwrap();
    let consumer = kafka::create_client(bootstrap_server);
    info!("Gathering offsets...");
    kafka::get_topic_offsets(&consumer, topic, &mut partitions, &mut start_offsets, &mut end_offsets);
    info!("Done.");
    partitions.sort();
    let mut mr = Metrics::new(partitions.len() as i32);

    info!("Start processing...");
    kafka::read_topic_into_metrics(topic, &consumer, &mut mr, &partitions, &end_offsets);

    let duration_secs = start_time.elapsed().as_secs();

    println!();
    println!("{}", "=".repeat(120));
    println!("Calculating statistics...");
    println!("Topic {}", topic);
    println!("Scanning took: {} seconds", duration_secs);
    println!("Estimated Msg/s: {}", (mr.overall_count() / duration_secs));
    println!("{}", "-".repeat(120));
    println!("Earliest Message: {}", mr.earliest_message());
    println!("Latest Message: {}", mr.latest_message());
    println!("{}", "-".repeat(120));
    println!("Largest Message: {} bytes", mr.largest_message());
    println!("Smallest Message: {} bytes", mr.smallest_message());
    println!("Topic Size: {} bytes", mr.overall_size());
    println!("{}", "=".repeat(120));

    let mut table = Table::new();
    table.add_row(row!["P", "|< OS", ">| OS", "Total", "Alive", "Tmb", "DR", "K Null", "K !Null", "P-Bytes", "K-Bytes", "V-Bytes", "A K-Sz", "A V-Sz", "A M-Sz"]);

    for partition in partitions {
        let key_size_avg = mr.key_size_avg(partition);
        table.add_row(Row::new(vec![
            Cell::new(format!("{}", partition).as_str()), // P
            Cell::new(format!("{}", &start_offsets[&partition]).as_str()), // |< OS
            Cell::new(format!("{}", &end_offsets[&partition]).as_str()), // OS >|
            Cell::new(format!("{}", mr.total(partition)).as_str()), // Total
            Cell::new(format!("{}", mr.alive(partition)).as_str()), // Alive
            Cell::new(format!("{}", mr.tombstones(partition)).as_str()), // TB
            Cell::new(format!("{0:.4}", mr.dirty_ratio(partition)).as_str()), // DR
            Cell::new(format!("{}", mr.key_null(partition)).as_str()), // K Null
            Cell::new(format!("{}", mr.key_non_null(partition)).as_str()), // K !Null
            Cell::new(format!("{}", mr.key_size_sum(partition) + mr.value_size_sum(partition)).as_str()), // P-Bytes
            Cell::new(format!("{}", mr.key_size_sum(partition)).as_str()), // K-Bytes
            Cell::new(format!("{}", mr.value_size_sum(partition)).as_str()), // V-Bytes
            Cell::new(format!("{}", key_size_avg).as_str()), // A-Key-Size
            Cell::new(format!("{}", mr.value_size_avg(partition)).as_str()), // A-V-Size
            Cell::new(format!("{}", mr.message_size_avg(partition)).as_str()), // A-M-Size
        ]));
    }

    println!("| K = Key, V = Value, P = Partition, Tmb = Tombstone(s), Sz = Size");
    println!("| DR = Dirty Ratio, A = Average, Lst = last, |< OS = start offset, >| OS = end offset");
    table.printstd();
    println!();
    println!("{}", "=".repeat(120));
}
