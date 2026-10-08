use std::convert::Infallible;
use std::future::Future;
use std::time::Duration;

use bytes::Bytes;
use http_body_util::Full;
use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Request, Response, StatusCode, header};
use hyper_util::rt::TokioIo;
use hyper_util::server::graceful::GracefulShutdown;
use tokio::net::TcpListener;
use tokio::task::JoinSet;

pub type Reply = Response<Full<Bytes>>;

pub async fn serve<F, Fut>(listener: TcpListener, handle: F, stop: impl Future<Output = ()>)
where
    F: Fn(Request<Incoming>) -> Fut + Clone + Send + 'static,
    Fut: Future<Output = Result<Reply, Infallible>> + Send + 'static,
{
    let graceful = GracefulShutdown::new();
    let mut connections = JoinSet::new();
    let signal = shutdown_signal();
    tokio::pin!(signal);
    tokio::pin!(stop);
    loop {
        tokio::select! {
            accepted = listener.accept() => match accepted {
                Ok((stream, _)) => {
                    let connection = http1::Builder::new()
                        .serve_connection(TokioIo::new(stream), service_fn(handle.clone()));
                    let connection = graceful.watch(connection);
                    connections.spawn(async move {
                        if let Err(error) = connection.await {
                            eprintln!("cassette connection: {error}");
                        }
                    });
                }
                Err(error) => {
                    eprintln!("cassette accept: {error}");
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            },
            Some(_) = connections.join_next(), if !connections.is_empty() => {}
            () = &mut stop => break,
            () = &mut signal => break,
        }
    }
    drop(listener);
    tokio::select! {
        () = graceful.shutdown() => {}
        () = tokio::time::sleep(Duration::from_secs(5)) => {}
    }
}

async fn shutdown_signal() {
    use tokio::signal::unix::{SignalKind, signal};
    match (
        signal(SignalKind::interrupt()),
        signal(SignalKind::terminate()),
    ) {
        (Ok(mut interrupt), Ok(mut terminate)) => {
            tokio::select! {
                _ = interrupt.recv() => {}
                _ = terminate.recv() => {}
            }
        }
        (Err(error), _) | (_, Err(error)) => {
            eprintln!("cassette: cannot watch signals: {error}");
            std::future::pending::<()>().await;
        }
    }
}

pub fn text(status: StatusCode, body: String) -> Reply {
    let mut reply = Response::new(Full::new(Bytes::from(body)));
    *reply.status_mut() = status;
    reply.headers_mut().insert(
        header::CONTENT_TYPE,
        header::HeaderValue::from_static("text/plain; charset=utf-8"),
    );
    reply
}
