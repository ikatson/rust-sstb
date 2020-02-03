use criterion::{criterion_group, criterion_main, BatchSize, Criterion};

use lsm::sstable::*;
use lsm::utils::SortedBytesIterator;

use rand::rngs::SmallRng;
use rand::seq::SliceRandom;
use rand::SeedableRng;
use std::iter::Iterator;

struct TestState {
    sorted_iter: SortedBytesIterator,
    shuffled: Vec<Vec<u8>>,
}

impl TestState {
    fn new(len: usize, limit: usize) -> Self {
        let mut it = SortedBytesIterator::new(len, limit);
        let shuffled = {
            let mut shuffled: Vec<Vec<u8>> = Vec::with_capacity(limit);
            while let Some(value) = it.next() {
                shuffled.push(value.into())
            }
            let mut small_rng = SmallRng::from_entropy();
            (&mut shuffled).shuffle(&mut small_rng);
            shuffled
        };

        it.reset();

        Self {
            sorted_iter: it,
            shuffled: shuffled,
        }
    }

    fn get_shuffled_input(&self) -> impl Iterator<Item = &[u8]> {
        self.shuffled.iter().map(|v| v as &[u8])
    }

    fn write_sstable(&self, filename: &str, write_opts: WriteOptions) -> Result<()> {
        let mut iter = self.sorted_iter.clone();

        let mut writer = writer::SSTableWriterV1::new_with_options(filename, write_opts)?;

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
        WriteOptions::builder()
            .compression(compression)
            .flush_every(flush)
            .build()
    };

    let filename = "/tmp/sstable";
    state.write_sstable(filename, make_write_opts(Compression::None, 4096)).unwrap();

    // Benchmark the full mmap implementation, that is thread safe.
    c.bench_function(&format!("full mmap,flush=4096 method=get items={}", items), |b| {
        b.iter_batched(
            || MmapUncompressedSSTableReader::new(filename).unwrap(),
            |reader| {
                for key in state.get_shuffled_input() {
                    let value = reader.get(key).unwrap();
                    assert_eq!(value, Some(key));
                }
            },
            BatchSize::LargeInput,
        );
    });

    for (prefix, write_opts, read_opts) in vec![
        (
            "mmap,compress=none,flush=4096,nocache",
            make_write_opts(Compression::None, 4096),
            ReadOptions {
                cache: None,
                use_mmap: true,
            },
        ),
        (
            "no_mmap,compress=none,flush=4096,nocache",
            make_write_opts(Compression::None, 4096),
            ReadOptions {
                cache: None,
                use_mmap: false,
            },
        ),
        (
            "no_mmap,compress=none,flush=4096,cache=unbounded",
            make_write_opts(Compression::None, 4096),
            ReadOptions {
                cache: Some(ReadCache::Unbounded),
                use_mmap: false,
            },
        ),
        (
            "no_mmap,compress=snappy,flush=65536,cache=unbounded",
            make_write_opts(Compression::Snappy, 8192),
            ReadOptions {
                cache: Some(ReadCache::Unbounded),
                use_mmap: false,
            },
        ),
        // ("mmap,compress=zlib,flush=65536,cache=32", make_write_opts(Compression::Snappy, 8192), ReadOptions{cache: Some(ReadCache::Blocks(32)), use_mmap: true}),
        // ("no_mmap,compress=zlib,flush=65536,cache=32", make_write_opts(Compression::Snappy, 8192), ReadOptions{cache: Some(ReadCache::Blocks(32)), use_mmap: false}),
        // ("no_mmap,compress=zlib,flush=65536,cache=unbounded", make_write_opts(Compression::Snappy, 8192), ReadOptions{cache: Some(ReadCache::Blocks(32)), use_mmap: false}),
    ]
    .into_iter()
    {

        state.write_sstable(filename, write_opts).unwrap();

        // c.bench_function(&format!("{} test=open items={}", prefix, items), |b| {
        //     b.iter(|| {
        //         SSTableReader::new_with_options(filename, &read_opts).unwrap()
        //     })
        // });

        c.bench_function(&format!("{} test=get items={}", prefix, items), |b| {
            b.iter_batched(
                || SSTableReader::new_with_options(filename, &read_opts).unwrap(),
                |mut reader| {
                    for key in state.get_shuffled_input() {
                        let value = reader.get(key).unwrap();
                        assert_eq!(value, Some(key));
                    }
                },
                BatchSize::LargeInput,
            );
        });
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
