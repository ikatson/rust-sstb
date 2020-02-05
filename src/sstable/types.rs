pub const VERSION_20: Version = Version { major: 2, minor: 0 };

use serde::{Serialize, Deserialize};

/// The version of the on-disk table.
#[derive(Serialize, Default, Deserialize, Debug, PartialEq, Eq)]
pub struct Version {
    major: u16,
    minor: u16,
}

/// Compression options for sstables.
#[derive(Serialize, Deserialize, Copy, Clone, Debug, PartialEq)]
pub enum Compression {
    None,
    Zlib,
    Snappy,
}

impl Default for Compression {
    fn default() -> Self {
        Self::None
    }
}