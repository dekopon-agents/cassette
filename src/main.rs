mod cassette;
mod error;
mod record;
mod replay;
mod server;

use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;

use clap::{Parser, Subcommand};
use hyper::header::HeaderName;
use tokio::net::TcpListener;

use crate::error::Error;
use crate::record::{Recorder, UpstreamArg, parse_upstream};
use crate::replay::Tape;

const DEFAULT_LISTEN: &str = "127.0.0.1:8787";

/// Record/replay HTTP server for Dekopon provider baseUrl.
///
/// One listener serves every provider under /NAME/PATH; point a provider's baseUrl at
/// http://HOST:PORT/NAME.
#[derive(Parser)]
#[command(version)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Forward each request to its upstream and save the exchange under DIR/NAME/.
    Record {
        /// Directory that receives one cassette file per exchange.
        #[arg(long)]
        dir: PathBuf,
        /// Upstream origin for requests under /NAME/; repeatable.
        #[arg(long = "upstream", value_name = "NAME=ORIGIN", required = true, num_args = 1.., value_parser = parse_upstream)]
        upstreams: Vec<UpstreamArg>,
        /// Request header saved as "[redacted]", beside authorization, proxy-authorization,
        /// cookie, x-api-key and api-key; repeatable.
        #[arg(long, value_name = "HEADER")]
        redact: Vec<HeaderName>,
        /// Address to listen on.
        #[arg(long, env = "CASSETTE_LISTEN", default_value = DEFAULT_LISTEN)]
        listen: SocketAddr,
    },
    /// Serve the exchanges saved under DIR; the first request with no recording left answers 501
    /// and ends the process with a failure status.
    Replay {
        /// Directory written by `cassette record`.
        #[arg(long)]
        dir: PathBuf,
        /// Address to listen on.
        #[arg(long, env = "CASSETTE_LISTEN", default_value = DEFAULT_LISTEN)]
        listen: SocketAddr,
    },
}

fn main() -> ExitCode {
    match run(Cli::parse().command) {
        Ok(code) => code,
        Err(error) => {
            eprintln!("cassette: {error}");
            ExitCode::from(2)
        }
    }
}

fn run(command: Command) -> Result<ExitCode, Error> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(Error::Runtime)?;
    match command {
        Command::Record {
            dir,
            upstreams,
            redact,
            listen,
        } => {
            let recorder = Arc::new(Recorder::open(&dir, upstreams, redact)?);
            runtime.block_on(async {
                let listener = bind(listen).await?;
                server::serve(
                    listener,
                    move |request| {
                        let recorder = Arc::clone(&recorder);
                        async move { recorder.handle(request).await }
                    },
                    std::future::pending(),
                )
                .await;
                Ok(ExitCode::SUCCESS)
            })
        }
        Command::Replay { dir, listen } => {
            let tape = Arc::new(Tape::load(&dir)?);
            runtime.block_on(async {
                let listener = bind(listen).await?;
                let handler = Arc::clone(&tape);
                server::serve(
                    listener,
                    move |request| {
                        let tape = Arc::clone(&handler);
                        async move { tape.handle(request).await }
                    },
                    tape.stopped(),
                )
                .await;
                Ok(if tape.missed() {
                    ExitCode::FAILURE
                } else {
                    ExitCode::SUCCESS
                })
            })
        }
    }
}

async fn bind(addr: SocketAddr) -> Result<TcpListener, Error> {
    let listener = TcpListener::bind(addr)
        .await
        .map_err(|source| Error::Listen { addr, source })?;
    let local = listener
        .local_addr()
        .map_err(|source| Error::Listen { addr, source })?;
    println!("cassette listening on http://{local}");
    Ok(listener)
}
