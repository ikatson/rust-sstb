#![warn(
    clippy::all,
    clippy::perf,
    clippy::cast_lossless,
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_precision_loss,

    // clippy::restriction,
    // clippy::pedantic,
    // clippy::nursery,
    // clippy::cargo,
)]

//! An experimental an educational attempt to write a Rust thread-safe sstables library.
//!
//! This was created as a learning excercise, to learn Rust, get some fair share of low-level
//! programming, optimization, to learn how sstables work and how to make them faster.
//!
//! By no means this is is complete or has any real-world usage.
//!
//! However inside are some working implementations that pass the tests, are thread-safe,
//! and even run smooth in benchmarks.
//!
//! The API is not stabilized, the disk format is not stabilized, there are no compatibility
//! guarantees or any other guarantees about this library.
//!
//! Use at your own risk.

pub mod sstable;
pub mod utils;

pub use sstable::*;

#[cfg(test)]
mod tests {}
