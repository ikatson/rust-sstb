use criterion::*;

use sstb::sstable::*;
use sstb::utils::SortedBytesIterator;

use rand::rngs::SmallRng;
use rand::seq::SliceRandom;
use rand::RngCore;
use rand::SeedableRng;
use std::iter::Iterator;

use rayon::prelude::*;

const ANY_BYTE: u8 = 130;

struct KV {
    key: Vec<u8>,
    is_present: bool,
}

struct TestState {
    sorted_iter: SortedBytesIterator,
    shuffled: Vec<KV>,
}

impl TestState {
    fn new(len: usize, limit: usize, percent_missing: f32) -> Self {
        let mut it = SortedBytesIterator::new(len, limit).unwrap();
        let shuffled = {
            let mut shuffled: Vec<KV> = Vec::with_capacity(limit * 2);
            let mut small_rng = SmallRng::from_seed(*b"seedseedseedseed");
            if percent_missing > 1f32 || percent_missing < 0f32 {
                panic!("expected 0 <= percent_missing <= 1")
            };
            let missing_threshold = (u32::max_value() as f32 * percent_missing) as u32;
            while let Some(value) = it.next() {
                if small_rng.next_u32() > missing_threshold {
                    let mut val = value.to_owned();
                    // whatever we push, it will alter the length and will be missing
                    val.push(ANY_BYTE);
                    shuffled.push(KV {
                        key: val,
                        is_present: false,
                    });
                }
                shuffled.push(KV {
                    key: value.into(),
                    is_present: true,
                })
            }

            (&mut shuffled).shuffle(&mut small_rng);
            shuffled
        };

        it.reset();

        Self {
            sorted_iter: it,
            shuffled,
        }
    }

    fn get_shuffled_input(&self) -> impl Iterator<Item = &KV> {
        self.shuffled.iter()
    }

    fn get_shuffled_input_ref(&self) -> &[KV] {
        &self.shuffled
    }

    fn get_input_iter(&self) -> SortedBytesIterator {
        self.sorted_iter.clone()
    }

    fn write_sstable(&self, filename: &str, write_opts: &WriteOptions) -> Result<()> {
        let mut iter = self.sorted_iter.clone();

        let mut writer = writer::SSTableWriterV2::new_with_options(filename, write_opts)?;

        while let Some(key) = iter.next() {
            writer.set(key, key)?;
        }

        writer.finish()
    }
}

fn compare_with_others(c: &mut Criterion) {
    let size = 100_000;
    use rocksdb::{DBCompressionType, Options, DB};
    let path = "/tmp/sstb";
    let rocks_path = "/tmp/rocksdb-rust-lsm";

    let bench = |group: &mut BenchmarkGroup<_>, state: &TestState, suffix: f32, threads| {
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(threads)
            .build()
            .unwrap();

        for (name, opts) in [
            ("rocksdb,mmap-reads,no-compression", {
                let mut opts = Options::default();
                opts.set_compression_type(DBCompressionType::None);
                opts.set_allow_mmap_reads(true);
                opts.create_if_missing(true);
                opts
            }),
            ("rocksdb,default", {
                let mut opts = Options::default();
                opts.create_if_missing(true);
                opts
            }),
        ]
        .iter()
        {
            {
                std::fs::remove_dir_all(rocks_path).unwrap();
                let db = DB::open(opts, rocks_path).unwrap();
                let mut iter = state.get_input_iter();
                while let Some(val) = iter.next() {
                    db.put(val, val).unwrap();
                }
                db.flush().unwrap();
            };
            group.bench_function(BenchmarkId::new(*name, suffix), |b| {
                b.iter_batched(
                    || DB::open(opts, rocks_path).unwrap(),
                    |db| {
                        pool.install(|| {
                            state.get_shuffled_input_ref().par_iter().for_each(|kv| {
                                let KV { key, is_present } = &kv;
                                let key = key as &[u8];

                                let value = db.get_pinned(key).unwrap();
                                if *is_present {
                                    assert_eq!(value.as_deref(), Some(key));
                                } else {
                                    assert!(value.is_none());
                                }
                            });
                        });
                    },
                    BatchSize::LargeInput,
                );
            });
        }

        for (name, opts) in [
            ("sstb,mmap-reads,no-compression", &WriteOptions::default()),
            (
                "sstb,mmap-reads,snappy",
                WriteOptions::default().compression(Compression::Snappy),
            ),
        ]
        .iter()
        {
            state.write_sstable(path, opts).unwrap();
            group.bench_function(BenchmarkId::new(*name, suffix), |b| {
                b.iter_batched(
                    || ConcurrentSSTableReader::new(path).unwrap(),
                    |db| {
                        pool.install(|| {
                            state.get_shuffled_input_ref().par_iter().for_each(|kv| {
                                let KV { key, is_present } = &kv;
                                let key = key as &[u8];

                                let value = db.get(key).unwrap();
                                if *is_present {
                                    assert_eq!(value.as_deref(), Some(key));
                                } else {
                                    assert_eq!(value, None);
                                }
                            });
                        });
                    },
                    BatchSize::LargeInput,
                );
            });
        }
    };

    let mut group = c.benchmark_group("compare with alternatives - threads");
    let state = TestState::new(32, size, 0.5);
    for threads in 1..=num_cpus::get_physical() {
        bench(&mut group, &state, threads as f32, threads);
    }
    group.finish();

    let mut group = c.benchmark_group("compare with alternatives - percent missing");
    for percent_missing in [0.1, 0.2, 0.4, 0.8, 0.95, 1.].iter() {
        let state = TestState::new(32, size, *percent_missing);
        let threads = num_cpus::get_physical();

        bench(&mut group, &state, *percent_missing, threads);
    }
    group.finish()
}

fn criterion_benchmark(c: &mut Criterion) {
    let make_write_opts = |compression, flush| {
        WriteOptions::default()
            .compression(compression)
            .flush_every(flush)
            .clone()
    };
    let filename = "/tmp/sstable";
    let variants = vec![
        (
            "mmap,compress=none,flush=4096,nocache",
            make_write_opts(Compression::None, 4096),
            ReadOptions::default().cache(None).use_mmap(true).clone(),
        ),
        (
            "mmap,compress=none,flush=4096,nocache,use_bloom=false",
            make_write_opts(Compression::None, 4096),
            ReadOptions::default()
                .cache(None)
                .use_mmap(true)
                .use_bloom(false)
                .clone(),
        ),
        (
            "no_mmap,compress=none,flush=4096,nocache",
            make_write_opts(Compression::None, 4096),
            ReadOptions::default().cache(None).use_mmap(false).clone(),
        ),
        (
            "no_mmap,compress=none,flush=4096,cache=unbounded",
            make_write_opts(Compression::None, 4096),
            ReadOptions::default()
                .cache(Some(ReadCache::Unbounded))
                .use_mmap(false)
                .clone(),
        ),
        (
            "mmap,compress=snappy,flush=8192,cache=unbounded",
            make_write_opts(Compression::Snappy, 8192),
            ReadOptions::default()
                .cache(Some(ReadCache::Unbounded))
                .use_mmap(true)
                .clone(),
        ),
        (
            "no_mmap,compress=snappy,flush=8192,nocache",
            make_write_opts(Compression::Snappy, 8192),
            ReadOptions::default().cache(None).use_mmap(false).clone(),
        ),
        (
            "no_mmap,compress=snappy,flush=8192,cache=unbounded",
            make_write_opts(Compression::Snappy, 8192),
            ReadOptions::default()
                .cache(Some(ReadCache::Unbounded))
                .use_mmap(false)
                .clone(),
        ),
        // ("mmap,compress=zlib,flush=65536,cache=32", make_write_opts(Compression::Snappy, 8192), ReadOptions{cache: Some(ReadCache::Blocks(32)), use_mmap: true}),
        // ("no_mmap,compress=zlib,flush=65536,cache=32", make_write_opts(Compression::Snappy, 8192), ReadOptions{cache: Some(ReadCache::Blocks(32)), use_mmap: false}),
        // ("no_mmap,compress=zlib,flush=65536,cache=unbounded", make_write_opts(Compression::Snappy, 8192), ReadOptions{cache: Some(ReadCache::Blocks(32)), use_mmap: false}),
    ];

    // Test single-threaded.
    let mut group = c.benchmark_group("method=get");
    let plot_config = PlotConfiguration::default().summary_scale(AxisScale::Logarithmic);
    group.plot_config(plot_config);

    for size in [100, 1000, 10_000, 100_000].iter() {
        let state = TestState::new(32, *size, 0.5);
        group.throughput(Throughput::Elements(*size as u64));
        state
            .write_sstable(filename, &make_write_opts(Compression::None, 4096))
            .unwrap();

        // Benchmark the full mmap implementation, that is thread safe.
        group.bench_function(
            BenchmarkId::new("MmapUncompressedSSTableReader,flush=4096", *size),
            |b| {
                b.iter_batched(
                    || MmapUncompressedSSTableReader::new(filename).unwrap(),
                    |reader| {
                        for kv in state.get_shuffled_input() {
                            let KV { key, is_present } = &kv;
                            let key = key as &[u8];
                            let value = reader.get(key).unwrap();
                            if *is_present {
                                assert_eq!(value, Some(key));
                            } else {
                                assert_eq!(value, None);
                            }
                        }
                    },
                    BatchSize::LargeInput,
                );
            },
        );

        for (prefix, write_opts, read_opts) in variants.iter() {
            state.write_sstable(filename, &write_opts).unwrap();

            group.bench_function(BenchmarkId::new(*prefix, *size), |b| {
                b.iter_batched(
                    || SSTableReader::new_with_options(filename, &read_opts).unwrap(),
                    |mut reader| {
                        for kv in state.get_shuffled_input() {
                            let KV { key, is_present } = &kv;
                            let key = key as &[u8];
                            let value = reader.get(key).unwrap();
                            if *is_present {
                                assert_eq!(value, Some(key));
                            } else {
                                assert_eq!(value, None);
                            }
                        }
                    },
                    BatchSize::LargeInput,
                );
            });
        }
    }
    group.finish();

    // Test multithreaded.
    let mut group = c.benchmark_group("method=get_multithreaded, 100 000 items");
    let size = 100_000;

    // Enabling throughput measuring here does not create a line chart somehow.
    // group.throughput(Throughput::Elements(size as u64));

    for threads in 1..=num_cpus::get_physical() {
        let state = TestState::new(32, size, 0.5);
        state
            .write_sstable(filename, &make_write_opts(Compression::None, 4096))
            .unwrap();

        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(threads)
            .build()
            .unwrap();

        group.bench_function(
            BenchmarkId::new("MmapUncompressedSSTableReader,flush=4096", threads),
            |b| {
                b.iter_batched(
                    || MmapUncompressedSSTableReader::new(filename).unwrap(),
                    |reader| {
                        pool.install(|| {
                            state.get_shuffled_input_ref().par_iter().for_each(|kv| {
                                let KV { key, is_present } = &kv;
                                let key = key as &[u8];
                                let value = reader.get(key).unwrap();
                                if *is_present {
                                    assert_eq!(value, Some(key));
                                } else {
                                    assert_eq!(value, None);
                                }
                            });
                        });
                    },
                    BatchSize::LargeInput,
                );
            },
        );

        for (prefix, write_opts, read_opts) in variants.iter() {
            state.write_sstable(filename, &write_opts).unwrap();

            group.bench_function(BenchmarkId::new(*prefix, threads), |b| {
                b.iter_batched(
                    || ConcurrentSSTableReader::new_with_options(filename, &read_opts).unwrap(),
                    |reader| {
                        pool.install(|| {
                            state.get_shuffled_input_ref().par_iter().for_each(|kv| {
                                let KV { key, is_present } = &kv;
                                let key = key as &[u8];
                                let value = reader.get(key).unwrap();
                                if *is_present {
                                    assert_eq!(value.as_deref(), Some(key));
                                } else {
                                    assert_eq!(value, None);
                                }
                            });
                        });
                    },
                    BatchSize::LargeInput,
                );
            });
        }
    }
    group.finish();
}

fn default_criterion() -> Criterion {
    Criterion::default().sample_size(10)
}

criterion_group! {
    name = sstable;
    config = default_criterion();
    targets = criterion_benchmark, compare_with_others
}

criterion_main!(sstable);
