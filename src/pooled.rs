use crate::atomic::SnowflakeIdGen as AtomicSnowflakeIdGen;
use std::{
    convert::TryInto,
    sync::atomic::{AtomicUsize, Ordering},
};

#[derive(Debug)]
pub struct SnowflakeIdGen {
    pub list: [AtomicSnowflakeIdGen; 32],
    pub next: AtomicUsize,
}

impl SnowflakeIdGen {
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        let wtf: [AtomicSnowflakeIdGen; 32] = (0..=31)
            .map(AtomicSnowflakeIdGen::new)
            .collect::<Vec<_>>()
            .try_into()
            .unwrap();

        SnowflakeIdGen {
            list: wtf,
            next: AtomicUsize::default(),
        }
    }

    pub fn next(&self) -> Option<i64> {
        let idx = self.next.fetch_add(1, Ordering::Relaxed);

        if idx >= 32 {
            self.next.store(0, Ordering::Release);
            self.list[0].generate()
        } else {
            self.list[idx].generate()
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
    const THREAD_COUNT: usize = 8;
    const IDS_PER_THREAD: usize = 1_000_000;

    #[test]
    fn no_duplication_between_multiple_threads() {
        let generator = Arc::new(SnowflakeIdGen::new());

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
                if let Some(id) = generator.next() {
                    break id;
                }
                println!("Thread {thread} Cycle {cycle}: idx for current time has been filled!");
                thread::sleep(Duration::from_millis(1));
            })
            // .inspect(|x| println!("{x:b}"))
            .collect::<Vec<_>>()
    }
}
