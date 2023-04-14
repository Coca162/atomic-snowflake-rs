use crate::get_time_millis;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

/// A atomic snowflake id generator
#[derive(Debug)]
pub struct SnowflakeIdGen {
    /// Epoch used to offset unix time, allows us to use less
    /// bits by not storing time where no ids will be made
    epoch: SystemTime,

    /// A atomic which stores a partial ID with the
    /// timestamp and the sequence, combining these
    /// two makes dealing with atomics less painful
    last_partial_id: AtomicU64,

    /// Identifies a unique generator in the id
    /// allowing for multiple generators to be used
    pub worker_id: u16,
}

impl SnowflakeIdGen {
    pub fn new(worker_id: u16) -> SnowflakeIdGen {
        Self::with_epoch(worker_id, UNIX_EPOCH)
    }

    pub fn with_epoch(worker_id: u16, epoch: SystemTime) -> SnowflakeIdGen {
        // TODO:limit the maximum bits of the worker_id
        let last_partial_id = get_time_millis(epoch) << 22 ;

        SnowflakeIdGen {
            epoch,
            last_partial_id: AtomicU64::new(last_partial_id),
            worker_id,
        }
    }

    pub fn generate(&self) -> Option<u64> {
        self.generate_with_millis_fn(get_time_millis)
    }

    #[inline(always)]
    fn generate_with_millis_fn<F>(&self, time_gen: F) -> Option<u64>
    where
        F: Fn(SystemTime) -> u64,
    {
        let new_partial_id = self
            .last_partial_id
            .fetch_update(Ordering::AcqRel, Ordering::Relaxed, |prev| {
                let timestamp_now_shifted = time_gen(self.epoch) << 22;

                match prev ^ timestamp_now_shifted {
                    4096.. => Some(timestamp_now_shifted),
                    4095 => None,
                    _ => Some(((prev & 0xFFF) + 1) | timestamp_now_shifted),
                }
            })
            .ok()?;

        Some(new_partial_id | ((self.worker_id as u64) << 17))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::iter;
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;

    const TOTAL_IDS: usize = IDS_PER_THREAD * THREAD_COUNT;
    const THREAD_COUNT: usize = 16;
    const IDS_PER_THREAD: usize = 2_000;

    #[test]
    fn no_duplication_between_multiple_threads() {
        let generator = Arc::new(SnowflakeIdGen::with_epoch(0, SystemTime::now()));

        let mut result = iter::repeat(generator)
            .enumerate()
            .take(THREAD_COUNT)
            .map(|data| thread::spawn(move || generate_many_ids(data)))
            // This collect makes it so the we don't go through all the threads one by one!!!
            .collect::<Vec<_>>()
            .into_iter()
            .fold(Vec::with_capacity(TOTAL_IDS), |mut vec, thread| {
                vec.append(&mut thread.join().unwrap());
                vec
            });

        result.sort();
        result.dedup();

        assert_eq!(TOTAL_IDS, result.len());
    }

    fn generate_many_ids((thread, generator): (usize, Arc<SnowflakeIdGen>)) -> Vec<u64> {
        (0..IDS_PER_THREAD)
            .map(|cycle| loop {
                if let Some(id) = generator.generate() {
                    break id;
                };
                println!("Thread {thread} Cycle {cycle}: idx for current time has been filled!");
                thread::sleep(Duration::from_millis(1));
            })
            .collect::<Vec<_>>()
    }

    #[test]
    fn fail_after_4095() {
        let generator = Arc::new(SnowflakeIdGen::with_epoch(0, SystemTime::now()));
        for _ in 1..=4095 {
            let id = generator.generate_with_millis_fn(|_| 0);
            assert!(matches!(id, Some(_)));
        }

        assert_eq!(generator.generate_with_millis_fn(|_| 0), None);
    }
}
