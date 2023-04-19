use crate::SnowflakeIdGen as AtomicSnowflakeIdGen;
use std::{
    convert::TryInto,
    sync::atomic::{AtomicUsize, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

/// A pooled version of the atomic SnowflakeIdGe.
/// Uses 5 out of the 10 instance bits to identify
/// the multiple generators and represents the left
/// over 5 as the `machine_id`
#[derive(Debug)]
pub struct SnowflakeIdGen {
    /// These are all the generators for a single machine
    /// we use 5 out of the 10 bits in the instance to uniquely
    /// identify these (and so that is why there is 32 of them)
    workers: [AtomicSnowflakeIdGen; 32],

    /// The next instance which will be pulled from the array
    next: AtomicUsize,

    /// A id which identifies a single machine which is running
    /// this program using the other 5 bits we left from the
    /// instance portion of the snowflake
    pub machine_id: u16,
}

impl SnowflakeIdGen {
    pub fn new(machine_id: u16) -> Self {
        SnowflakeIdGen::with_epoch(machine_id, UNIX_EPOCH)
    }

    pub fn with_epoch(machine_id: u16, epoch: SystemTime) -> Self {
        let workers: [AtomicSnowflakeIdGen; 32] = (0..=31)
            .map(|instance_id| machine_id | instance_id << 5)
            .map(|worker_id| AtomicSnowflakeIdGen::with_epoch(worker_id, epoch))
            .collect::<Vec<_>>()
            .try_into()
            .unwrap();

        SnowflakeIdGen {
            workers,
            next: AtomicUsize::default(),
            machine_id,
        }
    }

    pub fn next(&self) -> Option<u64> {
        let idx = self.next.fetch_add(1, Ordering::Relaxed);

        if idx >= 32 {
            self.next.store(0, Ordering::Release);
            self.workers[0].generate()
        } else {
            self.workers[idx].generate()
        }
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
        let generator = Arc::new(SnowflakeIdGen::new(0));

        let mut result = iter::repeat(generator)
            .enumerate()
            .take(THREAD_COUNT)
            .map(|data| thread::spawn(move || generate_many_ids(data)))
            .collect::<Vec<_>>()
            .into_iter()
            .fold(Vec::with_capacity(TOTAL_IDS), |mut vec, thread| {
                vec.append(&mut thread.join().unwrap());
                vec
            });

        println!("{}", result.len());

        result.sort();
        result.dedup();

        assert_eq!(TOTAL_IDS, result.len());
    }

    fn generate_many_ids((thread, generator): (usize, Arc<SnowflakeIdGen>)) -> Vec<u64> {
        (0..IDS_PER_THREAD)
            .map(|cycle| loop {
                if let Some(id) = generator.next() {
                    break id;
                };
                println!("Thread {thread} Cycle {cycle}: idx for current time has been filled!");
                thread::sleep(Duration::from_millis(1));
            })
            .collect::<Vec<_>>()
    }
}
