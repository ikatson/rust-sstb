use criterion::{criterion_group, criterion_main, BatchSize, Criterion};

use lsm::sstable::*;
use lsm::utils::SortedBytesIterator;

use rand::{Rng, SeedableRng};
use rand::rngs::SmallRng;
use rand::seq::SliceRandom;
use std::iter::Iterator;

struct TestState {
    sorted_iter: SortedBytesIterator,
    shuffled: Vec<Vec<u8>>
}

const ValueLen: usize = 1024;

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

        Self{
            sorted_iter: it,
            shuffled: shuffled,
        }
    }

    fn get_shuffled_input(&self) -> impl Iterator<Item=&[u8]> {
        self.shuffled.iter().map(|v| v as &[u8])
    }

    fn write_sstable(&self, filename: &str, write_opts: WriteOptions) -> Result<()> {
        let mut iter = self.sorted_iter.clone();

        let mut writer = writer::SSTableWriterV1::new_with_options(filename, write_opts)?;
        let buf = [0; ValueLen];

        while let Some(key) = iter.next() {
            writer.set(key, &buf)?;
        }

        writer.finish()
    }
}


// fn test_large_mmap_file_write_then_read() {
//     let mut write_opts = WriteOptions::default();
//     write_opts.compression = Compression::None;
//     let filename = "/tmp/sstable_big";
//     test_large_file_with_options(write_opts, ReadOptions::default(), filename, 800_000, true);
// }

// fn test_large_mmap_file_read() {
//     let filename = "/tmp/sstable_big";
//     test_large_file_with_options(WriteOptions::default(), ReadOptions::default(), filename, 800_000, false);
// }

// fn test_large_zlib_file_write_then_read() {
//     let mut opts = WriteOptions::default();
//     opts.compression = Compression::Zlib;
//     let filename = "/tmp/sstable_big_zlib";
//     test_large_file_with_options(opts, ReadOptions::default(), filename, 500_000, true);
// }

fn criterion_benchmark(c: &mut Criterion) {
    let items = 10_000;
    let state = TestState::new(10, items);

    let make_write_opts = |compression, flush| WriteOptions::builder().compression(compression).flush_every(flush).build();

    for (prefix, write_opts) in vec![
        ("compress=none,flush=4096", make_write_opts(Compression::None, 4096)),
        ("compress=none,flush=8192", make_write_opts(Compression::None, 8192)),
        ("compress=zlib,flush=4096", make_write_opts(Compression::Zlib, 4096)),
        ("compress=zlib,flush=8192", make_write_opts(Compression::Zlib, 8192)),
    ].into_iter() {
        let filename = "/tmp/sstable";
        state.write_sstable(filename, write_opts).unwrap();

        for (middle, read_opts) in [
            ("nocache", ReadOptions{cache: None}),
            ("cache=32", ReadOptions{cache: Some(ReadCache::Blocks(32))}),
            ("cache=unbounded", ReadOptions{cache: Some(ReadCache::Unbounded)}),
        ].iter() {
            // this takes forever
            if write_opts.compression == Compression::Zlib && read_opts.cache.is_none() {
                continue
            }
            c.bench_function(&format!("{} test=open {} items={}", prefix, middle, items), |b| {
                b.iter(|| {
                    SSTableReader::new_with_options(filename, &read_opts).unwrap()
                })
            });

            c.bench_function(&format!("{} test=get {} items={}", prefix, middle, items), |b| {
                b.iter_batched(
                    || {
                        SSTableReader::new_with_options(
                            filename,
                            &read_opts
                        ).unwrap()
                    },
                    |mut reader| {

                        for key in state.get_shuffled_input() {
                            let value = reader.get(key).unwrap();
                            assert_eq!(value.map(|b| b.len()), Some(ValueLen));
                        }
                    },
                    BatchSize::LargeInput
                );
            });
        }
    }
}

fn default_criterion() -> Criterion {
    Criterion::default().sample_size(10)
}

criterion_group!{
    name = sstable;
    config = default_criterion();
    targets = criterion_benchmark
}

criterion_main!(sstable);
