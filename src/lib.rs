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

//! A future implementation of LSM tree will be in this module.
//! As of the time of writing this, it contains only the `sstable` implementation.

pub mod sstable;
pub mod utils;

#[cfg(test)]
mod tests {}
