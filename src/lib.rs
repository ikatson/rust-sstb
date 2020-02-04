#![warn(
    // clippy::all,
    // clippy::perf,
    clippy::cast_lossless,
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_precision_loss,

    // clippy::restriction,
    // clippy::pedantic,




    // clippy::nursery,
    // clippy::cargo,
)]

pub mod sstable;
pub mod utils;

#[cfg(test)]
mod tests {}
