use metric::Metrics;
use rdkafka::client::ClientContext;
use rdkafka::config::{ClientConfig, RDKafkaLogLevel};
use rdkafka::consumer::{Consumer, ConsumerContext};
use rdkafka::consumer::BaseConsumer;
use rdkafka::error::KafkaResult;
use rdkafka::types::RDKafkaTopicPartitionList;
use std::collections::HashMap;
use std::time::Duration;
use uuid::Uuid;
use chrono::Utc;
use rdkafka::message::Message;
use chrono::prelude::*;
use rdkafka::consumer::DefaultConsumerContext;

pub type KafkaConsumer = BaseConsumer<DefaultConsumerContext>;

pub fn create_client(bootstrap_server: &str) -> KafkaConsumer {
    ClientConfig::new()
        .set("group.id", format!("topic-analyzer--{}-{}", env!("USER"), Uuid::new_v4()).as_str())
        .set("bootstrap.servers", bootstrap_server)
        .set("enable.partition.eof", "false")
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "false")
        .set("api.version.request", "true")
        .set("enable.auto.offset.store", "false")
        .set("client.id", "topic-analyzer")
        .set("queue.buffering.max.ms", "1000")

        .set_log_level(RDKafkaLogLevel::Info)
        .create()
        .expect("Consumer creation failed")
}

pub fn get_topic_offsets(consumer: &KafkaConsumer, topic: &str, parts: &mut Vec<i32>, start_offsets: &mut HashMap<i32, i64>, end_offsets: &mut HashMap<i32, i64>) {
    let md = consumer.fetch_metadata(Option::from(topic), Duration::new(10, 0)).unwrap_or_else(|e| { panic!("Error fetching metadata: {}", e) });
    let topic_metadata = md.topics().first().unwrap_or_else(|| { panic!("Topic not found!") });

    for partition in topic_metadata.partitions() {
        parts.push(partition.id());
        let (low, high) = consumer.fetch_watermarks(topic, partition.id(), Duration::new(1, 0)).unwrap();
        start_offsets.insert(partition.id(), low);
        end_offsets.insert(partition.id(), high);
    }
}

pub fn read_topic_into_metrics(topic: &str,
                               consumer: &KafkaConsumer,
                               metrics: &mut Metrics,
                               partitions: &[i32],
                               end_offsets: &HashMap<i32, i64>) {
    let mut seq: u64 = 0;
    let mut still_running = HashMap::<i32, bool>::new();
    for &p in partitions {
        still_running.insert(p, true);
    }

    info!("Subscribing to {}", topic);
    consumer.subscribe(&[topic]).expect("Can't subscribe to specified topic");

    info!("Starting message consumption...");
    loop {
        match consumer.poll(Duration::from_millis(100)) {
            None => {}
            Some(Err(e)) => {
                warn!("Kafka error: {}", e);
            }
            Some(Ok(m)) => {
                seq += 1;
                let partition = m.partition();
                let offset = m.offset();
                let timestamp = DateTime::<Utc>::from_utc(NaiveDateTime::from_timestamp(m.timestamp().to_millis().unwrap() / 1000, 0), Utc);
                let mut message_size: u64 = 0;
                let mut empty_key = false;
                let mut empty_value = false;

                metrics.inc_overall_count();
                metrics.inc_total(partition);

                match m.key() {
                    Some(k) => {
                        metrics.inc_key_non_null(partition);
                        let k_len = k.len() as u64;
                        message_size += k_len;
                        metrics.inc_key_size_sum(partition, k_len);
                        metrics.inc_overall_size(k_len);
                    }
                    None => {
                        empty_key = true;
                        metrics.inc_key_null(partition);
                    }
                }

                match m.payload() {
                    Some(v) => {
                        let v_len = v.len() as u64;
                        message_size += v_len;
                        metrics.inc_value_size_sum(partition, v_len);
                        metrics.inc_overall_size(v_len);
                        metrics.inc_alive(partition);
                    }
                    None => {
                        empty_value = true;
                        metrics.inc_tombstones(partition);
                    }
                }

                metrics.cmp_and_set_message_timestamp(timestamp);

                if !empty_key && !empty_value {
                    metrics.cmp_and_set_message_size(message_size);
                }

                if seq % 100_000 == 0 {
                    info!("[Sq: {} | T: {} | P: {} | O: {} | Ts: {}]",
                          seq, topic, partition, offset, timestamp);
                }

                if let Err(e) = consumer.store_offset(&m) {
                    warn!("Error while storing offset: {}", e);
                }

                if (offset + 1) >= *end_offsets.get(&partition).unwrap() {
                    *still_running.get_mut(&partition).unwrap() = false;
                }

                let mut all_done = true;
                for running in still_running.values() {
                    if *running {
                        all_done = false;
                    }
                }

                if all_done {
                    break;
                }
            }
        }
    }
}
