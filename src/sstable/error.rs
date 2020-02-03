use super::Version;

pub const INVALID_DATA: Error = Error::InvalidData("corrupt SStable or bug");

#[derive(Debug)]
pub enum Error {
    Io(std::io::Error),
    ProgrammingError(&'static str),
    InvalidData(&'static str),
    UnsupportedVersion(Version),
    Bincode(bincode::Error),
    Utf8Error(std::str::Utf8Error),
    KeyTooLong(usize),
    ValueTooLong(usize),
    StdStringFromUtf8Error(std::string::FromUtf8Error),
    NixError(nix::Error),
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

impl From<std::string::FromUtf8Error> for Error {
    fn from(e: std::string::FromUtf8Error) -> Self {
        Error::StdStringFromUtf8Error(e)
    }
}

impl From<nix::Error> for Error {
    fn from(e: nix::Error) -> Self {
        Error::NixError(e)
    }
}
