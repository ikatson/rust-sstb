pub const VERSION_10: Version = Version { major: 1, minor: 0 };

use serde::{Serialize, Deserialize};

#[derive(Serialize, Default, Deserialize, Debug, PartialEq, Eq)]
pub struct Version {
    major: u16,
    minor: u16,
}

#[derive(Serialize, Deserialize, Copy, Clone, Debug, PartialEq)]
pub enum Compression {
    None,
    Zlib,
    Snappy,
}

impl Default for Compression {
    fn default() -> Self {
        Compression::None
    }
}