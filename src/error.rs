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
    Echoed {
        path: PathBuf,
    },
    Runtime(io::Error),
    DuplicateUpstreams(Vec<String>),
    ProxyNotHttp {
        origin: String,
    },
    ProxyMalformed {
        variable: &'static str,
    },
    Tls(rustls::Error),
}

impl fmt::Display for Error {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Io { path, source } => write!(formatter, "{}: {source}", path.display()),
            Error::Parse { path, source } => write!(formatter, "{}: {source}", path.display()),
            Error::Invalid { path, reason } => write!(formatter, "{}: {reason}", path.display()),
            Error::Listen { addr, source } => write!(formatter, "listen on {addr}: {source}"),
            Error::Echoed { path } => write!(
                formatter,
                "{}: not saved, the upstream response echoes a redacted request header",
                path.display()
            ),
            Error::Runtime(source) => write!(formatter, "start the runtime: {source}"),
            Error::DuplicateUpstreams(names) => {
                write!(
                    formatter,
                    "upstream named more than once: {}",
                    names.join(", ")
                )
            }
            Error::ProxyNotHttp { origin } => write!(
                formatter,
                "the proxy for {origin} must be an http:// URL (CONNECT is sent in clear)"
            ),
            Error::ProxyMalformed { variable } => {
                write!(formatter, "{variable} is not a proxy URL")
            }
            Error::Tls(source) => write!(formatter, "TLS configuration: {source}"),
        }
    }
}

impl std::error::Error for Error {}
