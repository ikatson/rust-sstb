use criterion::{criterion_group, criterion_main, BatchSize, Criterion};

use sstb::sstable::*;
use sstb::utils::SortedBytesIterator;

use rand::rngs::SmallRng;
use rand::RngCore;
use rand::seq::SliceRandom;
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
    fn new(len: usize, limit: usize) -> Self {
        let mut it = SortedBytesIterator::new(len, limit).unwrap();
        let (shuffled, rng) = {
            let mut shuffled: Vec<KV> = Vec::with_capacity(limit * 2);
            let mut small_rng = SmallRng::from_seed(*b"seedseedseedseed");
            let missing_threshold = u32::max_value() / 2;
            while let Some(value) = it.next() {
                if small_rng.next_u32() > missing_threshold {
                    let mut val = value.to_owned();
                    // whatever we push, it will alter the length and will be missing
                    val.push(ANY_BYTE);
                    shuffled.push(KV{key: val, is_present: false});
                }
                shuffled.push(KV{key: value.into(), is_present: true})
            }

            (&mut shuffled).shuffle(&mut small_rng);
            (shuffled, small_rng)
        };

        it.reset();

        Self {
            sorted_iter: it,
            shuffled,
            rng,
        }
    }

    fn get_shuffled_input(&self) -> impl Iterator<Item = &KV> {
        self.shuffled.iter()
    }

    fn get_shuffled_input_ref(&self) -> &[KV] {
        &self.shuffled
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

fn criterion_benchmark(c: &mut Criterion) {
    let items = 100_000;
    let state = TestState::new(32, items);

    let make_write_opts = |compression, flush| {
        WriteOptions::default()
            .compression(compression)
            .flush_every(flush)
            .clone()
    };

    let filename = "/tmp/sstable";
    state
        .write_sstable(filename, &make_write_opts(Compression::None, 4096))
        .unwrap();

    // Benchmark the full mmap implementation, that is thread safe.
    c.bench_function(
        &format!("MmapUncompressedSSTableReader,flush=4096 method=get items={}", items),
        |b| {
            b.iter_batched(
                || MmapUncompressedSSTableReader::new(filename).unwrap(),
                |reader| {
                    for kv in state.get_shuffled_input() {
                        let KV{key, is_present} = &kv;
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

    c.bench_function(
        &format!(
            "MmapUncompressedSSTableReader,flush=4096 method=get_multithreaded items={}",
            items
        ),
        |b| {
            b.iter_batched(
                || MmapUncompressedSSTableReader::new(filename).unwrap(),
                |reader| {
                    state.get_shuffled_input_ref().par_iter().for_each(|kv| {
                        let KV{key, is_present} = &kv;
                        let key = key as &[u8];
                        let value = reader.get(key).unwrap();
                        if *is_present {
                            assert_eq!(value, Some(key));
                        } else {
                            assert_eq!(value, None);
                        }
                    });
                },
                BatchSize::LargeInput,
            );
        },
    );

    for (prefix, write_opts, read_opts) in vec![
        (
            "mmap,compress=none,flush=4096,nocache",
            make_write_opts(Compression::None, 4096),
            ReadOptions::default().cache(None).use_mmap(true).clone(),
        ),

        (
            "mmap,compress=none,flush=4096,nocache,use_bloom=false",
            make_write_opts(Compression::None, 4096),
            ReadOptions::default().cache(None).use_mmap(true).use_bloom(false).clone(),
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
    ]
    .into_iter()
    {
        state.write_sstable(filename, &write_opts).unwrap();

        c.bench_function(&format!("{},test=get,items={}", prefix, items), |b| {
            b.iter_batched(
                || SSTableReader::new_with_options(filename, &read_opts).unwrap(),
                |mut reader| {
                    for kv in state.get_shuffled_input() {
                        let KV{key, is_present} = &kv;
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

        c.bench_function(
            &format!("{},test=get_multithreaded,items={}", prefix, items),
            |b| {
                b.iter_batched(
                    || ConcurrentSSTableReader::new_with_options(filename, &read_opts).unwrap(),
                    |reader| {
                        state.get_shuffled_input_ref().par_iter().for_each(|kv| {
                            let KV{key, is_present} = &kv;
                            let key = key as &[u8];
                            let value = reader.get(key).unwrap();
                            if *is_present {
                                assert_eq!(value.as_ref().map(|b| b.as_ref()), Some(key));
                            } else {
                                assert_eq!(value, None);
                            }
                        });
                    },
                    BatchSize::LargeInput,
                );
            },
        );
    }
}

fn default_criterion() -> Criterion {
    Criterion::default().sample_size(10)
}

criterion_group! {
    name = sstable;
    config = default_criterion();
    targets = criterion_benchmark
}

criterion_main!(sstable);
