use std::collections::HashMap;
use chrono::prelude::*;
use fnv32::FnvHasher;
use kafka::MetricHandler;
use rdkafka::message::{Message, BorrowedMessage};
use bit_set::BitSet;

type Partition = i32;
type PartitionedCounterBucket = HashMap<Partition, u64>;

#[derive(Debug, Clone)]
pub struct MessageMetrics {
    total_messages: PartitionedCounterBucket,
    tombstones: PartitionedCounterBucket,
    alive: PartitionedCounterBucket,
    key_null: PartitionedCounterBucket,
    key_non_null: PartitionedCounterBucket,
    key_size_sum: PartitionedCounterBucket,
    value_size_sum: PartitionedCounterBucket,
    earliest_message: DateTime<Utc>,
    latest_message: DateTime<Utc>,
    smallest_message: u64,
    largest_message: u64,
    overall_size: u64,
    overall_count: u64,
}


impl MessageMetrics {
    pub fn new() -> MessageMetrics {
        MessageMetrics {
            total_messages: PartitionedCounterBucket::new(),
            tombstones: PartitionedCounterBucket::new(),
            alive: PartitionedCounterBucket::new(),
            key_null: PartitionedCounterBucket::new(),
            key_non_null: PartitionedCounterBucket::new(),
            key_size_sum: PartitionedCounterBucket::new(),
            value_size_sum: PartitionedCounterBucket::new(),
            earliest_message: Utc::now(),
            latest_message: DateTime::from_utc(NaiveDateTime::from_timestamp(0, 0), Utc),
            largest_message: 0,
            smallest_message: <u64>::max_value(),
            overall_size: 0,
            overall_count: 0,
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
            self.latest_message = cmp;
        }
    }

    pub fn inc_total(&mut self, p: Partition) {
        *self.total_messages.entry(p).or_insert(0u64) += 1;
    }

    pub fn inc_tombstones(&mut self, p: Partition) {
        *self.tombstones.entry(p).or_insert(0u64) += 1;
    }

    pub fn inc_alive(&mut self, p: Partition) {
        *self.alive.entry(p).or_insert(0u64) += 1;
    }

    pub fn inc_key_null(&mut self, p: Partition) {
        *self.key_null.entry(p).or_insert(0u64) += 1;
    }

    pub fn inc_key_non_null(&mut self, p: Partition) {
        *self.key_non_null.entry(p).or_insert(0u64) += 1;
    }

    pub fn inc_key_size_sum(&mut self, p: Partition, amount: u64) {
        *self.key_size_sum.entry(p).or_insert(0u64) += amount;
    }

    pub fn inc_value_size_sum(&mut self, p: Partition, amount: u64) {
        *self.value_size_sum.entry(p).or_insert(0u64) += amount;
    }

    ////////////////////////////////////////////////////////////////

    pub fn total(&self, p: Partition) -> u64 {
        self.metric(&self.total_messages, p)
    }

    pub fn tombstones(&self, p: Partition) -> u64 {
        self.metric(&self.tombstones, p)
    }

    pub fn alive(&self, p: Partition) -> u64 {
        self.metric(&self.alive, p)
    }

    pub fn key_null(&self, p: Partition) -> u64 {
        self.metric(&self.key_null, p)
    }

    pub fn key_non_null(&self, p: Partition) -> u64 {
        self.metric(&self.key_non_null, p)
    }

    pub fn key_size_sum(&self, p: Partition) -> u64 {
        self.metric(&self.key_size_sum, p)
    }

    pub fn value_size_sum(&self, p: Partition) -> u64 {
        self.metric(&self.value_size_sum, p)
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
        if self.smallest_message == <u64>::max_value() {
            0
        } else {
            self.smallest_message
        }
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

    #[inline]
    fn metric(&self, bucket: &PartitionedCounterBucket, p: Partition) -> u64 {
        match bucket.get(&p) {
            Some(v) => *v,
            None => 0
        }
    }
}

impl MetricHandler for MessageMetrics {
    fn handle_message<'b>(&mut self, m: &BorrowedMessage<'b>) where BorrowedMessage<'b>: Message {
        let partition = m.partition();
        let timestamp = m.timestamp().to_millis().unwrap_or(0);
        let parsed_naive_timestamp = NaiveDateTime::from_timestamp(timestamp / 1000, 0);
        let timestamp_dt = DateTime::<Utc>::from_utc(parsed_naive_timestamp, Utc);
        let mut message_size: u64 = 0;
        let mut empty_value = false;

        self.inc_overall_count();
        self.inc_total(partition);

        match m.key() {
            Some(k) => {
                self.inc_key_non_null(partition);
                let k_len = k.len() as u64;
                message_size += k_len;
                self.inc_key_size_sum(partition, k_len);
                self.inc_overall_size(k_len);
                k
            }
            None => {
                self.inc_key_null(partition);
                &[]
            }
        };

        match m.payload() {
            Some(v) => {
                let v_len = v.len() as u64;
                message_size += v_len;
                self.inc_value_size_sum(partition, v_len);
                self.inc_overall_size(v_len);
                self.inc_alive(partition);
            }
            None => {
                empty_value = true;
                self.inc_tombstones(partition);
            }
        }

        self.cmp_and_set_message_timestamp(timestamp_dt);

        if !empty_value {
            self.cmp_and_set_message_size(message_size);
        }
    }
}


fn fnv1a(bytes: &[u8]) -> usize {
    let mut hasher = FnvHasher::default();
    hasher.write(bytes);
    hasher.finish() as usize // FIXME: this makes the binary incompatible with any non 64bit system
}

pub struct LogCompactionInMemoryMetrics {
    store: Box<BitSet>
}

impl LogCompactionInMemoryMetrics {
    pub fn new() -> LogCompactionInMemoryMetrics {
        LogCompactionInMemoryMetrics {
            store: Box::new(BitSet::new())
        }
    }

    pub fn mark_key_alive(&mut self, key: &[u8]) {
        let k = fnv1a(key);
        self.store.insert(k);
    }

    pub fn mark_key_dead(&mut self, key: &[u8]) {
        self.store.remove(fnv1a(key));
    }

    pub fn sum_all_alive(&self) -> usize {
        self.store.len()
    }
}


impl MetricHandler for LogCompactionInMemoryMetrics {
    fn handle_message<'b>(&mut self, m: &BorrowedMessage<'b>) where BorrowedMessage<'b>: Message {
        // No counting for un-keyed topics
        match m.key() {
            Some(k) => {
                match m.payload() {
                    Some(_) => {
                        self.mark_key_alive(k);
                    },
                    None => {
                        self.mark_key_dead(k);
                    }
                }
            }
            None => {}
        }
    }
}
