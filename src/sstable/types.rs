pub const VERSION_30: Version = Version { major: 3, minor: 0 };

use serde::{Deserialize, Serialize};

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
