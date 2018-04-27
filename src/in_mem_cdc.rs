use std::hash::Hasher;
use fnv::FnvHasher;
use bitmap::{Bitmap, OneBit};

fn fnv1a(bytes: &[u8]) -> usize {
    let mut hasher = FnvHasher::default();
    hasher.write(bytes);
    hasher.finish() as usize // FIXME: this makes the binary incompatible with any non 64bit system
}

struct LogCompactionInMemoryMetrics {
    store: Bitmap<Vec<usize>, OneBit>
}

impl LogCompactionInMemoryMetrics {
    pub fn new() -> LogCompactionInMemoryMetrics {
        LogCompactionInMemoryMetrics {
            store: Bitmap::from_storage(<usize>::max_value(), OneBit, vec![]).unwrap()
        }
    }

    pub fn mark_key_alive(&mut self, key: &[u8]) {
        self.store.set(fnv1a(key), 1);
    }

    pub fn mark_key_dead(&mut self, key: &[u8]) {
        self.store.set(fnv1a(key), 0);
    }

    pub fn sum_all_alive(&self) -> u64 {
        let mut valid = 0u64;
        for value in self.store.iter() {
            if value == 1 {
                valid += 1;
            }
        }
        valid
    }
}

#[test]
fn test_bitmap() {}
