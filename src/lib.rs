use std::time::{SystemTime, UNIX_EPOCH};

pub mod atomic;
pub mod pooled;

/// A unsynchronized snowflake generator
#[derive(Copy, Clone, Debug)]
pub struct SnowflakeIdGen {
    /// Epoch used to offset unix time, allows us to use less
    /// bits by not storing time where no ids will be made
    epoch: SystemTime,

    /// The last time a ID was generated in milliseconds
    last_time_millis: i64,

    /// Identifies a unique generator in the id which
    /// allows for multiple generators to be used
    pub worker_id: i32,

    /// Auto-incremented for every ID generated in the same millisecond
    sequence: u16,
}

impl SnowflakeIdGen {
    pub fn new(worker_id: i32) -> SnowflakeIdGen {
        Self::with_epoch(worker_id, UNIX_EPOCH)
    }

    pub fn with_epoch(worker_id: i32, epoch: SystemTime) -> SnowflakeIdGen {
        //TODO:limit the maximum of input args machine_id and node_id
        let last_time_millis = get_time_millis(epoch);

        SnowflakeIdGen {
            epoch,
            last_time_millis,
            worker_id,
            sequence: 0,
        }
    }

    pub fn generate(&mut self) -> Option<i64> {
        self.generate_with_millis_fn(get_time_millis)
    }

    #[inline(always)]
    fn generate_with_millis_fn<F>(&mut self, time_gen: F) -> Option<i64>
    where
        F: Fn(SystemTime) -> i64,
    {
        let now_millis = time_gen(self.epoch);

        if now_millis == self.last_time_millis {
            if self.sequence >= 4095 {
                return None;
            }
        } else {
            self.last_time_millis = now_millis;
            self.sequence = 0;
        }

        self.sequence += 1;

        Some(self.last_time_millis << 22 | ((self.worker_id << 17) as i64) | (self.sequence as i64))
    }
}

#[inline(always)]
/// Get the latest milliseconds of the clock.
pub fn get_time_millis(epoch: SystemTime) -> i64 {
    SystemTime::now()
        .duration_since(epoch)
        .expect("The epoch is later then now")
        .as_millis() as i64
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::iter;
    use std::sync::{Arc, Mutex};
    use std::thread;
    use std::time::Duration;

    const TOTAL_IDS: usize = IDS_PER_THREAD * THREAD_COUNT;
    const THREAD_COUNT: usize = 16;
    const IDS_PER_THREAD: usize = 2_000;

    #[test]
    fn no_duplication_between_multiple_threads() {
        let generator = Arc::new(Mutex::new(SnowflakeIdGen::with_epoch(0, SystemTime::now())));

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

    fn generate_many_ids((thread, generator): (usize, Arc<Mutex<SnowflakeIdGen>>)) -> Vec<i64> {
        (0..IDS_PER_THREAD)
            .map(|cycle| loop {
                let mut lock = generator.lock().unwrap();

                if let Some(id) = lock.generate() {
                    break id;
                }
                println!("Thread {thread} Cycle {cycle}: idx for current time has been filled!");
                drop(lock);
                thread::sleep(Duration::from_millis(1));
            })
            // .inspect(|x| println!("{x:b}"))
            .collect::<Vec<_>>()
    }

    #[test]
    fn fail_after_4095() {
        let mut generator = SnowflakeIdGen::with_epoch(0, SystemTime::now());

        for _ in 1..=4095 {
            let id = generator.generate_with_millis_fn(|_| 0);
            assert!(matches!(id, Some(_)));
        }

        assert_eq!(generator.generate_with_millis_fn(|_| 0), None);
    }
}
