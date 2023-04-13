use crate::get_time_millis;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

/// The `SnowflakeIdGen` type is snowflake algorithm wrapper.
#[derive(Debug)]
pub struct SnowflakeIdGen {
    /// epoch used by the snowflake algorithm.
    epoch: SystemTime,

    /// last_time_millis, last time generate id is used times millis.
    last_millis_idx: AtomicI64,

    /// instance, is use to supplement id machine or sectionalization attribute.
    pub instance: i32,
}

impl SnowflakeIdGen {
    pub fn new(instance: i32) -> SnowflakeIdGen {
        Self::with_epoch(instance, UNIX_EPOCH)
    }

    pub fn with_epoch(instance: i32, epoch: SystemTime) -> SnowflakeIdGen {
        //TODO:limit the maximum of input args machine_id and node_id
        let last_time_millis = get_time_millis(epoch);

        SnowflakeIdGen {
            epoch,
            last_millis_idx: AtomicI64::new(last_time_millis << 22),
            instance,
        }
    }

    pub fn generate(&self) -> Option<i64> {
        self.generate_with_millis_fn(get_time_millis)
    }

    fn generate_with_millis_fn<F>(&self, f: F) -> Option<i64>
    where
        F: Fn(SystemTime) -> i64,
    {
        let new = self
            .last_millis_idx
            .fetch_update(Ordering::AcqRel, Ordering::Relaxed, |prev| {
                let now_shifted = f(self.epoch) << 22;

                match prev ^ now_shifted {
                    4096.. => Some(now_shifted),
                    4095 => None,
                    _ => Some(((prev & 0xFFF) + 1) | now_shifted),
                }
            })
            .ok()?;

        Some(new | ((self.instance << 12) as i64))
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

    fn generate_many_ids((thread, generator): (usize, Arc<SnowflakeIdGen>)) -> Vec<i64> {
        (0..IDS_PER_THREAD)
            .map(|cycle| loop {
                if let Some(id) = generator.generate() {
                    break id;
                }
                println!("Thread {thread} Cycle {cycle}: idx for current time has been filled!");
                thread::sleep(Duration::from_millis(1));
            })
            // .inspect(|x| println!("{x:b}"))
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
