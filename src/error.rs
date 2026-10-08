use std::fmt;
use std::io;
use std::net::SocketAddr;
use std::path::PathBuf;

#[derive(Debug)]
pub enum Error {
    Io {
        path: PathBuf,
        source: io::Error,
    },
    Parse {
        path: PathBuf,
        source: serde_json::Error,
    },
    Invalid {
        path: PathBuf,
        reason: String,
    },
    Listen {
        addr: SocketAddr,
        source: io::Error,
    },
    Runtime(io::Error),
    DuplicateUpstreams(Vec<String>),
}

impl fmt::Display for Error {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Io { path, source } => write!(formatter, "{}: {source}", path.display()),
            Error::Parse { path, source } => write!(formatter, "{}: {source}", path.display()),
            Error::Invalid { path, reason } => write!(formatter, "{}: {reason}", path.display()),
            Error::Listen { addr, source } => write!(formatter, "listen on {addr}: {source}"),
            Error::Runtime(source) => write!(formatter, "start the runtime: {source}"),
            Error::DuplicateUpstreams(names) => {
                write!(
                    formatter,
                    "upstream named more than once: {}",
                    names.join(", ")
                )
            }
        }
    }
}

impl std::error::Error for Error {}
