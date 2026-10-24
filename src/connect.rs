//! The outbound connector `record` uses: direct TCP, or a CONNECT tunnel through the proxy that
//! `HTTPS_PROXY`/`HTTP_PROXY`/`ALL_PROXY` name for the destination (minus `NO_PROXY` hosts), with
//! TLS to the upstream inside it. Roots are the built-in webpki ones plus any in `SSL_CERT_FILE`,
//! so a proxy that terminates TLS with its own CA is trusted when that CA is handed over.

use std::error::Error as StdError;
use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use hyper::Uri;
use hyper::http::uri::{Authority, Scheme};
use hyper_rustls::HttpsConnector;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::client::legacy::connect::proxy::Tunnel;
use hyper_util::client::proxy::matcher::Matcher;
use hyper_util::rt::TokioIo;
use rustls::pki_types::CertificateDer;
use rustls::pki_types::pem::PemObject;
use tokio::net::TcpStream;
use tower_service::Service;

use crate::error::Error;

type BoxError = Box<dyn StdError + Send + Sync>;

/// Builds the HTTPS-or-HTTP connector from the process environment, checking that every origin
/// that will be proxied names an `http://` proxy.
pub fn from_env(origins: &[&str]) -> Result<HttpsConnector<ProxyConnector>, Error> {
    let proxies = Matcher::from_env();
    for origin in origins {
        let Ok(uri) = origin.parse::<Uri>() else {
            continue;
        };
        if let Some(proxy) = proxies.intercept(&uri)
            && proxy.uri().scheme() != Some(&Scheme::HTTP)
        {
            return Err(Error::Proxy(format!(
                "the proxy for {origin} must be an http:// URL (CONNECT is sent in clear)"
            )));
        }
    }

    let mut direct = HttpConnector::new();
    direct.enforce_http(false);
    let connector = ProxyConnector {
        direct,
        proxies: Arc::new(proxies),
    };
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let tls = rustls::ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(|error| Error::Proxy(format!("TLS configuration: {error}")))?
        .with_root_certificates(roots()?)
        .with_no_client_auth();
    Ok(hyper_rustls::HttpsConnectorBuilder::new()
        .with_tls_config(tls)
        .https_or_http()
        .enable_http1()
        .wrap_connector(connector))
}

fn roots() -> Result<rustls::RootCertStore, Error> {
    let mut roots = rustls::RootCertStore {
        roots: webpki_roots::TLS_SERVER_ROOTS.to_vec(),
    };
    let Some(file) = std::env::var_os("SSL_CERT_FILE").filter(|file| !file.is_empty()) else {
        return Ok(roots);
    };
    let path = PathBuf::from(file);
    let invalid = |reason: String| Error::Invalid {
        path: path.clone(),
        reason: format!("SSL_CERT_FILE: {reason}"),
    };
    let certificates = CertificateDer::pem_file_iter(&path)
        .map_err(|error| invalid(error.to_string()))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| invalid(error.to_string()))?;
    let (added, _) = roots.add_parsable_certificates(certificates);
    if added == 0 {
        return Err(invalid("holds no usable PEM certificate".to_owned()));
    }
    Ok(roots)
}

#[derive(Clone)]
pub struct ProxyConnector {
    direct: HttpConnector,
    proxies: Arc<Matcher>,
}

impl Service<Uri> for ProxyConnector {
    type Response = TokioIo<TcpStream>;
    type Error = BoxError;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, BoxError>> + Send>>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), BoxError>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, destination: Uri) -> Self::Future {
        let mut direct = self.direct.clone();
        let Some(proxy) = self.proxies.intercept(&destination) else {
            return Box::pin(async move { direct.call(destination).await.map_err(Into::into) });
        };
        let mut tunnel = Tunnel::new(proxy.uri().clone(), direct);
        if let Some(auth) = proxy.basic_auth() {
            tunnel = tunnel.with_auth(auth.clone());
        }
        Box::pin(async move {
            // Tunnel assumes 443 when the port is implicit; an http:// origin means 80.
            let destination = with_port(destination)?;
            tunnel.call(destination).await.map_err(Into::into)
        })
    }
}

fn with_port(uri: Uri) -> Result<Uri, BoxError> {
    let Some(authority) = uri
        .authority()
        .filter(|authority| authority.port().is_none())
    else {
        return Ok(uri);
    };
    let port = if uri.scheme() == Some(&Scheme::HTTP) {
        80
    } else {
        443
    };
    let authority: Authority = format!("{}:{port}", authority.host()).parse()?;
    let mut parts = uri.into_parts();
    parts.authority = Some(authority);
    Ok(Uri::from_parts(parts)?)
}
