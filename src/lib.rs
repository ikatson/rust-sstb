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

pub mod sstable;
pub mod utils;

pub use sstable::*;

#[cfg(test)]
mod tests {}
