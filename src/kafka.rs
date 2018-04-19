use metric::Metrics;
use std::collections::HashMap;
use std::time::Duration;
use chrono::prelude::*;
use chrono::Utc;
use indicatif::{ProgressBar, ProgressStyle};
use rdkafka::config::{ClientConfig, RDKafkaLogLevel};
use rdkafka::consumer::{BaseConsumer, Consumer, DefaultConsumerContext};
use rdkafka::message::{Message, BorrowedMessage};
use uuid::Uuid;

pub type KafkaConsumer = BaseConsumer<DefaultConsumerContext>;

pub struct TopicAnalyzer<'a> {
    consumer: KafkaConsumer,
    metric_handlers: Vec<&'a MetricHandler>,
}

pub trait MetricHandler {
    fn handle_message<'b>(&self, m: &BorrowedMessage<'b>) where BorrowedMessage<'b>: Message;
}

impl <'a> TopicAnalyzer<'a> {
    pub fn new_from_bootstrap_servers(bootstrap_server: &str) -> TopicAnalyzer<'a> {
        TopicAnalyzer {
            consumer: ClientConfig::new()
                // we use ENV["USER"] to make the analyzer identify-able. It can emit quite a lot of load
                // on the cluster, so you might want to see who this is.
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
                .expect("Consumer creation failed"),
            metric_handlers: vec![],
        }
    }

    pub fn add_metric_handler(&mut self, handler: &'a MetricHandler) {
        self.metric_handlers.push(handler);
    }

    pub fn get_topic_offsets(&self, topic: &str) -> (HashMap<i32, i64>, HashMap<i32, i64>) {
        let md = self.consumer.fetch_metadata(Option::from(topic), Duration::new(10, 0)).unwrap_or_else(|e| { panic!("Error fetching metadata: {}", e) });
        let topic_metadata = md.topics().first().unwrap_or_else(|| { panic!("Topic not found!") });

        let mut start_offsets = HashMap::<i32, i64>::new();
        let mut end_offsets = HashMap::<i32, i64>::new();
        for partition in topic_metadata.partitions() {
            let (low, high) = self.consumer.fetch_watermarks(topic, partition.id(), Duration::new(1, 0)).unwrap();
            start_offsets.insert(partition.id(), low);
            end_offsets.insert(partition.id(), high);
        }
        (start_offsets, end_offsets)
    }

    pub fn read_topic_into_metrics(&mut self,
                                   topic: &str,
                                   end_offsets: &HashMap<i32, i64>) {
        let mut seq: u64 = 0;
        let mut still_running = HashMap::<i32, bool>::new();
        for &p in end_offsets.keys() {
            still_running.insert(p, true);
        }

        println!("Subscribing to {}", topic);
        self.consumer.subscribe(&[topic]).expect("Can't subscribe to specified topic");

        println!("Starting message consumption...");
        let sty = ProgressStyle::default_spinner().template("{spinner} [{elapsed_precise}] {msg}");
        let pb = ProgressBar::new_spinner();
        pb.set_style(sty.clone());

        loop {
            match self.consumer.poll(Duration::from_millis(100)) {
                None => {}
                Some(Err(e)) => {
                    warn!("Kafka error: {}", e);
                }
                Some(Ok(m)) => {
                    seq += 1;
                    let partition = m.partition();
                    let offset = m.offset();
                    let parsed_naive_timestamp = NaiveDateTime::from_timestamp(m.timestamp().to_millis().unwrap() / 1000, 0);
                    let timestamp = DateTime::<Utc>::from_utc(parsed_naive_timestamp, Utc);

                    for mh in self.metric_handlers.iter_mut() {
                        mh.handle_message(&m);
                    }

                    pb.inc(1);
                    pb.set_message(
                        format!("[Sq: {} | T: {} | P: {} | O: {} | Ts: {}]",
                                seq, topic, partition, offset, timestamp).as_str());

                    if let Err(e) = self.consumer.store_offset(&m) {
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
                        pb.finish_with_message("done");
                        break;
                    }
                }
            }
        }
    }
}
