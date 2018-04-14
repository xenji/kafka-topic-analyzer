use std::collections::HashMap;
use chrono::prelude::*;

type Partition = i32;
type PartitionedCounterBucket = HashMap<Partition, u64>;
type MetricRegistry = HashMap<String, PartitionedCounterBucket>;

#[derive(Debug)]
pub struct Metrics {
    registry: MetricRegistry,
    earliest_message: DateTime<Utc>,
    latest_message: DateTime<Utc>,
    smallest_message: u64,
    largest_message: u64,
    overall_size: u64,
    overall_count: u64
}

impl Metrics {
    pub fn new(number_of_partitions: i32) -> Metrics {
        let mut mr = MetricRegistry::new();
        let keys = vec![
            "topic.messages.total",
            "topic.messages.tombstones",
            "topic.messages.alive",
            "topic.messages.key.null",
            "topic.messages.key.non-null",
            "topic.messages.key-size.sum",
            "topic.messages.value-size.sum"];
        for key in keys {
            let mut pcb = PartitionedCounterBucket::new();
            for i in 0..number_of_partitions {
                pcb.insert(i, 0u64);
            }
            mr.insert(String::from(key), pcb);
        }
        Metrics {
            registry: mr,
            earliest_message: Utc::now(),
            latest_message: DateTime::from_utc(NaiveDateTime::from_timestamp(0, 0), Utc),
            largest_message: 0,
            smallest_message: <u64>::max_value(),
            overall_size: 0,
            overall_count: 0
        }
    }

    pub fn inc_overall_size(&mut self, amount: u64) {
        self.overall_size += amount;
    }

    pub fn inc_overall_count(&mut self) {
        self.overall_count += 1;
    }

    pub fn cmp_and_set_message_size(&mut self, size: u64) {
        if self.largest_message < size {
            self.largest_message = size;
        }
        if self.smallest_message > size {
            self.smallest_message = size;
        }
    }

    pub fn cmp_and_set_message_timestamp(&mut self, cmp: DateTime<Utc>) {
        if self.earliest_message.gt(&cmp) {
            self.earliest_message = cmp;
        }
        if self.latest_message.lt(&cmp) {
            self.latest_message= cmp;
        }
    }

    pub fn inc_total(&mut self, p: Partition) {
        self.increment("topic.messages.total", p, 1);
    }

    pub fn inc_tombstones(&mut self, p: Partition) {
        self.increment("topic.messages.tombstones", p, 1);
    }

    pub fn inc_alive(&mut self, p: Partition) {
        self.increment("topic.messages.alive", p, 1);
    }

    pub fn inc_key_null(&mut self, p: Partition) {
        self.increment("topic.messages.key.null", p, 1);
    }

    pub fn inc_key_non_null(&mut self, p: Partition) {
        self.increment("topic.messages.key.non-null", p, 1);
    }

    pub fn inc_key_size_sum(&mut self, p: Partition, amount: u64) {
        self.increment("topic.messages.key-size.sum", p, amount);
    }

    pub fn inc_value_size_sum(&mut self, p: Partition, amount: u64) {
        self.increment("topic.messages.value-size.sum", p, amount);
    }
    ////////////////////////////////////////////////////////////////

    pub fn total(&self, p: Partition) -> u64 {
        self.metric("topic.messages.total", p)
    }

    pub fn tombstones(&self, p: Partition) -> u64 {
        self.metric("topic.messages.tombstones", p)
    }

    pub fn alive(&self, p: Partition) -> u64 {
        self.metric("topic.messages.alive", p)
    }

    pub fn key_null(&self, p: Partition) -> u64 {
        self.metric("topic.messages.key.null", p)
    }

    pub fn key_non_null(&self, p: Partition) -> u64 {
        self.metric("topic.messages.key.non-null", p)
    }

    pub fn key_size_sum(&self, p: Partition) -> u64 {
        self.metric("topic.messages.key-size.sum", p)
    }

    pub fn value_size_sum(&self, p: Partition) -> u64 {
        self.metric("topic.messages.value-size.sum", p)
    }

    pub fn key_size_avg(&self, p: Partition) -> u64 {
        let key_size_sum = self.key_size_sum(p);
        if key_size_sum > 0 {
            key_size_sum / self.alive(p)
        } else {
            0
        }
    }

    pub fn value_size_avg(&self, p: Partition) -> u64 {
        let value_size_sum = self.value_size_sum(p);
        if value_size_sum > 0 {
            value_size_sum / self.alive(p)
        } else {
            0
        }
    }

    pub fn message_size_avg(&self, p: Partition) -> u64 {
        let msg_size_sum = self.key_size_sum(p) + self.value_size_sum(p);
        if msg_size_sum > 0 {
            msg_size_sum / self.alive(p)
        } else {
            0
        }
    }

    pub fn dirty_ratio(&self, p: Partition) -> f32 {
        let total_messages = self.total(p);
        let tombstones = self.tombstones(p);
        if total_messages > 0 && tombstones > 0 {
            tombstones as f32 / (total_messages as f32 / 100.0f32)
        } else {
            0.0f32
        }
    }

    pub fn latest_message(&self) -> &DateTime<Utc> {
        &self.latest_message
    }

    pub fn earliest_message(&self) -> &DateTime<Utc> {
        &self.earliest_message
    }

    pub fn smallest_message(&self) -> u64 {
        self.smallest_message
    }

    pub fn largest_message(&self) -> u64 {
        self.largest_message
    }

    pub fn overall_count(&self) -> u64 {
        self.overall_count
    }

    pub fn overall_size(&self) -> u64 {
        self.overall_size
    }

    fn metric(&self, key: &str, p: Partition) -> u64 {
        self.registry[key][&p]
    }

    fn increment(&mut self, key: &str, p: Partition, amount: u64) {
        *self.registry.get_mut(key).unwrap().get_mut(&p).unwrap() += amount;
    }
}

#[test]
fn test_metrics() {
    let mut mr = Metrics::new(10);
    mr.inc_total(0);
    mr.inc_total(1);
    mr.inc_total(1);
    assert_eq!(*mr.registry.get("topic.messages.total").unwrap().get(&0).unwrap(), 1u64);
    assert_eq!(*mr.registry.get("topic.messages.total").unwrap().get(&1).unwrap(), 2u64);
}
