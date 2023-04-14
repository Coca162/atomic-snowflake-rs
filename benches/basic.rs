use criterion::{black_box, criterion_group, criterion_main, Criterion};
use std::iter;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

const TOTAL_IDS: usize = IDS_PER_THREAD * THREAD_COUNT;
const THREAD_COUNT: usize = 16;
const IDS_PER_THREAD: usize = 2_000;

fn bench_pools(c: &mut Criterion) {
    let generator_atomic = Arc::new(snowflake::atomic::SnowflakeIdGen::new(0));
    let generator_mutex = Arc::new(Mutex::new(snowflake::SnowflakeIdGen::new(0)));
    let generator_pool = Arc::new(snowflake::pooled::SnowflakeIdGen::new(0));

    let mut group = c.benchmark_group("Snowflakes");
    group.significance_level(0.02);
    group.warm_up_time(Duration::from_secs(10));
    group.measurement_time(Duration::from_secs(30));
    group.sample_size(1000);
    group.bench_function("atomic_snowflake", move |b| {
        b.iter(|| atomic_snowflake(black_box(generator_atomic.clone())))
    });
    group.bench_function("mutex_snowflake", move |b| {
        b.iter(|| mutex_snowflake(black_box(generator_mutex.clone())))
    });
    group.bench_function("pool_atomic_snowflake", move |b| {
        b.iter(|| pool_atomic_snowflake(black_box(generator_pool.clone())))
    });
    group.finish();
}

fn atomic_snowflake(generator: Arc<snowflake::atomic::SnowflakeIdGen>) -> Vec<u64> {
    iter::repeat(generator)
        .take(THREAD_COUNT)
        .map(|data| thread::spawn(move || atomic_generate_many_ids(data)))
        // This collect makes it so the we don't go through all the threads one by one!!!
        .collect::<Vec<_>>()
        .into_iter()
        .fold(Vec::with_capacity(TOTAL_IDS), |mut vec, thread| {
            vec.append(&mut thread.join().unwrap());
            vec
        })
}

fn atomic_generate_many_ids(generator: Arc<snowflake::atomic::SnowflakeIdGen>) -> Vec<u64> {
    (0..IDS_PER_THREAD)
        .map(|_| loop {
            if let Some(id) = generator.generate() {
                break id;
            }
            // println!("Thread {thread} Cycle {cycle}: idx for current time has been filled!");
            thread::sleep(Duration::from_millis(1));
        })
        .collect::<Vec<_>>()
}

fn mutex_snowflake(generator: Arc<Mutex<snowflake::SnowflakeIdGen>>) -> Vec<u64> {
    iter::repeat(generator)
        .take(THREAD_COUNT)
        .map(|data| thread::spawn(move || mutex_generate_many_ids(data)))
        // This collect makes it so the we don't go through all the threads one by one!!!
        .collect::<Vec<_>>()
        .into_iter()
        .fold(Vec::with_capacity(TOTAL_IDS), |mut vec, thread| {
            vec.append(&mut thread.join().unwrap());
            vec
        })
}

fn mutex_generate_many_ids(generator: Arc<Mutex<snowflake::SnowflakeIdGen>>) -> Vec<u64> {
    (0..IDS_PER_THREAD)
        .map(|_| loop {
            let mut lock = generator.lock().unwrap();

            if let Some(id) = lock.generate() {
                break id;
            }
            // println!("Thread {thread} Cycle {cycle}: idx for current time has been filled!");
            drop(lock);
            thread::sleep(Duration::from_millis(1));
        })
        .collect::<Vec<_>>()
}

fn pool_atomic_snowflake(generator: Arc<snowflake::pooled::SnowflakeIdGen>) -> Vec<u64> {
    iter::repeat(generator)
        .take(THREAD_COUNT)
        .map(|data| thread::spawn(move || pool_atomic_many_ids(data)))
        // This collect makes it so the we don't go through all the threads one by one!!!
        .collect::<Vec<_>>()
        .into_iter()
        .fold(Vec::with_capacity(TOTAL_IDS), |mut vec, thread| {
            vec.append(&mut thread.join().unwrap());
            vec
        })
}

fn pool_atomic_many_ids(generator: Arc<snowflake::pooled::SnowflakeIdGen>) -> Vec<u64> {
    (0..IDS_PER_THREAD)
        .map(|_| loop {
            // println!("wut");
            if let Some(id) = generator.next() {
                break id;
            }
            // println!("Thread {thread} Cycle {cycle}: idx for current time has been filled!");
            thread::sleep(Duration::from_millis(1));
        })
        // .inspect(|x| println!("I am respectfully looking {x:b}"))
        .collect::<Vec<_>>()
}

criterion_group!(benches, bench_pools);
criterion_main!(benches);
