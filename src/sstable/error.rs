use super::Version;

#[derive(Debug)]
pub enum Error {
    Io(std::io::Error),
    InvalidData(&'static str),
    UnsupportedVersion(Version),
    Bincode(bincode::Error),
    Utf8Error(std::str::Utf8Error)
}

impl Error {
    pub fn invalid_data(msg: &'static str) -> Self {
        Error::InvalidData(msg)
    }
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Error::Io(e)
    }
}

impl From<bincode::Error> for Error {
    fn from(e: bincode::Error) -> Self {
        Error::Bincode(e)
    }
}

impl From<std::str::Utf8Error> for Error {
    fn from(e: std::str::Utf8Error) -> Self {
        Error::Utf8Error(e)
    }
}