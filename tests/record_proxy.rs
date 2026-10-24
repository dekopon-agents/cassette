use std::convert::Infallible;
use std::io::{BufRead, BufReader, Read};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::Arc;
use std::time::Duration;

use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::header::{self, HeaderValue};
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Request, Response, StatusCode};
use hyper_util::client::legacy::Client;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::rt::{TokioExecutor, TokioIo};
use parking_lot::Mutex;
use rcgen::{BasicConstraints, CertificateParams, IsCa, Issuer, KeyPair};
use rustls::pki_types::{PrivateKeyDer, PrivatePkcs8KeyDer};
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;
use tokio_rustls::TlsAcceptor;

const SECRET: &str = "ghp-synthetic-9c2e7d41a0";
const PROXY_USER: &str = "sandbox";
const PROXY_SECRET: &str = "proxy-synthetic-5b8f13";

const PROXY_VARIABLES: [&str; 8] = [
    "HTTPS_PROXY",
    "https_proxy",
    "HTTP_PROXY",
    "http_proxy",
    "ALL_PROXY",
    "all_proxy",
    "NO_PROXY",
    "no_proxy",
];

struct Pki {
    ca_pem: String,
    tls: Arc<rustls::ServerConfig>,
}

fn pki() -> Pki {
    let ca_key = KeyPair::generate().unwrap();
    let mut ca_params = CertificateParams::new(Vec::<String>::new()).unwrap();
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    let ca = ca_params.self_signed(&ca_key).unwrap();
    let issuer = Issuer::new(ca_params, ca_key);

    let leaf_key = KeyPair::generate().unwrap();
    let leaf = CertificateParams::new(vec!["127.0.0.1".to_owned()])
        .unwrap()
        .signed_by(&leaf_key, &issuer)
        .unwrap();
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let tls = rustls::ServerConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_no_client_auth()
        .with_single_cert(
            vec![leaf.der().clone()],
            PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(leaf_key.serialize_der())),
        )
        .unwrap();
    Pki {
        ca_pem: ca.pem(),
        tls: Arc::new(tls),
    }
}

struct Server {
    port: u16,
    seen: Arc<Mutex<Vec<String>>>,
    task: JoinHandle<()>,
}

impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn tls_upstream(tls: Arc<rustls::ServerConfig>) -> Server {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let seen = Arc::new(Mutex::new(Vec::new()));
    let authorizations = Arc::clone(&seen);
    let acceptor = TlsAcceptor::from(tls);
    let task = tokio::spawn(async move {
        loop {
            let (stream, _) = listener.accept().await.unwrap();
            let acceptor = acceptor.clone();
            let authorizations = Arc::clone(&authorizations);
            tokio::spawn(async move {
                // A client that rejects the certificate ends the handshake; that is a test outcome.
                let Ok(stream) = acceptor.accept(stream).await else {
                    return;
                };
                let service = service_fn(move |request: Request<Incoming>| {
                    let authorizations = Arc::clone(&authorizations);
                    async move {
                        if let Some(value) = request.headers().get(header::AUTHORIZATION) {
                            authorizations
                                .lock()
                                .push(value.to_str().unwrap().to_owned());
                        }
                        let body = json!({"full_name": "dekopon-agents/cassette"}).to_string();
                        let mut response = Response::new(Full::new(Bytes::from(body)));
                        response.headers_mut().insert(
                            header::CONTENT_TYPE,
                            HeaderValue::from_static("application/json"),
                        );
                        Ok::<_, Infallible>(response)
                    }
                });
                http1::Builder::new()
                    .serve_connection(TokioIo::new(stream), service)
                    .await
                    .unwrap();
            });
        }
    });
    Server { port, seen, task }
}

async fn connect_proxy() -> Server {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let seen = Arc::new(Mutex::new(Vec::new()));
    let heads = Arc::clone(&seen);
    let task = tokio::spawn(async move {
        loop {
            let (mut client, _) = listener.accept().await.unwrap();
            let heads = Arc::clone(&heads);
            tokio::spawn(async move {
                // The client sends nothing after the head until it reads our 200.
                let mut head = Vec::new();
                while !head.ends_with(b"\r\n\r\n") {
                    let mut byte = [0];
                    if client.read(&mut byte).await.unwrap() == 0 {
                        return;
                    }
                    head.push(byte[0]);
                }
                let head = String::from_utf8(head).unwrap();
                heads.lock().push(head.clone());
                let target = head
                    .strip_prefix("CONNECT ")
                    .and_then(|rest| rest.split_once(' '))
                    .map(|(target, _)| target.to_owned());
                let Some(target) = target else {
                    client
                        .write_all(b"HTTP/1.1 405 Method Not Allowed\r\n\r\n")
                        .await
                        .unwrap();
                    return;
                };
                let Ok(mut upstream) = TcpStream::connect(target).await else {
                    client
                        .write_all(b"HTTP/1.1 502 Bad Gateway\r\n\r\n")
                        .await
                        .unwrap();
                    return;
                };
                client
                    .write_all(b"HTTP/1.1 200 Connection Established\r\n\r\n")
                    .await
                    .unwrap();
                // Either side may close first; the splice ending is all that matters.
                if let Err(error) = tokio::io::copy_bidirectional(&mut client, &mut upstream).await
                {
                    eprintln!("proxy splice: {error}");
                }
            });
        }
    });
    Server { port, seen, task }
}

struct Recorder {
    child: Child,
    base: String,
}

impl Recorder {
    fn start(dir: &Path, upstream: &str, env: &[(&str, &str)]) -> Recorder {
        let mut command = Command::new(env!("CARGO_BIN_EXE_cassette"));
        command
            .args(["record", "--dir", dir.to_str().unwrap()])
            .args(["--upstream", upstream])
            .args(["--listen", "127.0.0.1:0"])
            .env_remove("SSL_CERT_FILE")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        for variable in PROXY_VARIABLES {
            command.env_remove(variable);
        }
        command.envs(env.iter().copied());
        let mut child = command.spawn().unwrap();
        let mut line = String::new();
        BufReader::new(child.stdout.take().unwrap())
            .read_line(&mut line)
            .unwrap();
        let base = line
            .trim()
            .strip_prefix("cassette listening on ")
            .unwrap_or_else(|| panic!("unexpected first line {line:?}"))
            .to_owned();
        Recorder { child, base }
    }
}

impl Drop for Recorder {
    fn drop(&mut self) {
        if let Err(error) = self.child.kill() {
            eprintln!("kill cassette: {error}");
        }
        if let Err(error) = self.child.wait() {
            eprintln!("wait for cassette: {error}");
        }
    }
}

async fn get(url: &str, authorization: &str) -> (StatusCode, Bytes) {
    let client: Client<HttpConnector, Full<Bytes>> =
        Client::builder(TokioExecutor::new()).build_http();
    let request = Request::get(url)
        .header(header::AUTHORIZATION, authorization)
        .header(header::ACCEPT, "application/json")
        .body(Full::new(Bytes::new()))
        .unwrap();
    let response = client.request(request).await.unwrap();
    let status = response.status();
    let body = response.into_body().collect().await.unwrap().to_bytes();
    (status, body)
}

fn files(dir: &Path) -> Vec<PathBuf> {
    let mut found = Vec::new();
    for entry in std::fs::read_dir(dir).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            found.extend(files(&path));
        } else {
            found.push(path);
        }
    }
    found.sort();
    found
}

fn contains(haystack: &[u8], needle: &str) -> bool {
    haystack
        .windows(needle.len())
        .any(|window| window == needle.as_bytes())
}

struct Setup {
    work: tempfile::TempDir,
    ca_file: PathBuf,
    upstream: Server,
    proxy: Server,
}

impl Setup {
    async fn new() -> Setup {
        let work = tempfile::tempdir().unwrap();
        let pki = pki();
        let ca_file = work.path().join("ca.pem");
        std::fs::write(&ca_file, &pki.ca_pem).unwrap();
        Setup {
            upstream: tls_upstream(pki.tls).await,
            proxy: connect_proxy().await,
            ca_file,
            work,
        }
    }

    fn cassettes(&self) -> PathBuf {
        self.work.path().join("cassettes")
    }

    fn origin(&self) -> String {
        format!("gh=https://127.0.0.1:{}", self.upstream.port)
    }

    fn proxy_url(&self) -> String {
        format!(
            "http://{PROXY_USER}:{PROXY_SECRET}@127.0.0.1:{}",
            self.proxy.port
        )
    }
}

#[tokio::test]
async fn recording_tunnels_through_https_proxy_and_trusts_ssl_cert_file() {
    let setup = Setup::new().await;
    let proxy = setup.proxy_url();
    let recorder = Recorder::start(
        &setup.cassettes(),
        &setup.origin(),
        &[
            ("HTTPS_PROXY", &proxy),
            ("SSL_CERT_FILE", setup.ca_file.to_str().unwrap()),
        ],
    );
    let bearer = format!("Bearer {SECRET}");

    let (status, body) = get(
        &format!("{}/gh/repos/dekopon-agents/cassette", recorder.base),
        &bearer,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{}", String::from_utf8_lossy(&body));
    assert_eq!(
        serde_json::from_slice::<Value>(&body).unwrap(),
        json!({"full_name": "dekopon-agents/cassette"})
    );

    let target = format!("127.0.0.1:{}", setup.upstream.port);
    let heads = setup.proxy.seen.lock().clone();
    assert_eq!(heads.len(), 1, "{heads:?}");
    let proxy_credential = STANDARD.encode(format!("{PROXY_USER}:{PROXY_SECRET}"));
    assert!(
        heads[0].starts_with(&format!("CONNECT {target} HTTP/1.1\r\n")),
        "{heads:?}"
    );
    assert!(
        heads[0].contains(&format!(
            "proxy-authorization: Basic {proxy_credential}\r\n"
        )) || heads[0].contains(&format!(
            "Proxy-Authorization: Basic {proxy_credential}\r\n"
        )),
        "{heads:?}"
    );
    assert_eq!(*setup.upstream.seen.lock(), vec![bearer]);

    let written = files(&setup.cassettes());
    assert_eq!(
        written,
        [setup
            .cassettes()
            .join("gh/0001-GET-repos-dekopon-agents-cassette.json")]
    );
    let bytes = std::fs::read(&written[0]).unwrap();
    let saved: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(saved["request"]["headers"]["authorization"], "[redacted]");
    assert_eq!(
        saved["response"]["body"],
        json!({"json": {"full_name": "dekopon-agents/cassette"}})
    );
    for secret in [SECRET, PROXY_SECRET, proxy_credential.as_str()] {
        assert!(
            !contains(&bytes, secret),
            "{} holds a credential",
            written[0].display()
        );
    }
}

#[tokio::test]
async fn without_ssl_cert_file_the_test_ca_is_not_trusted() {
    let setup = Setup::new().await;
    let proxy = setup.proxy_url();
    let recorder = Recorder::start(
        &setup.cassettes(),
        &setup.origin(),
        &[("HTTPS_PROXY", &proxy)],
    );

    let (status, body) = get(
        &format!("{}/gh/repos/x", recorder.base),
        &format!("Bearer {SECRET}"),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_GATEWAY);
    assert!(
        String::from_utf8_lossy(&body).contains("certificate"),
        "{}",
        String::from_utf8_lossy(&body)
    );
    assert_eq!(setup.proxy.seen.lock().len(), 1);
    assert!(setup.upstream.seen.lock().is_empty());
    assert_eq!(files(&setup.cassettes()), Vec::<PathBuf>::new());
}

#[tokio::test]
async fn no_proxy_hosts_are_reached_directly() {
    let setup = Setup::new().await;
    let proxy = setup.proxy_url();
    let recorder = Recorder::start(
        &setup.cassettes(),
        &setup.origin(),
        &[
            ("HTTPS_PROXY", &proxy),
            ("NO_PROXY", "example.com,127.0.0.1"),
            ("SSL_CERT_FILE", setup.ca_file.to_str().unwrap()),
        ],
    );

    let (status, body) = get(
        &format!("{}/gh/repos/x", recorder.base),
        &format!("Bearer {SECRET}"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{}", String::from_utf8_lossy(&body));
    assert!(setup.proxy.seen.lock().is_empty());
    assert_eq!(files(&setup.cassettes()).len(), 1);
}

fn startup_failure(env: &[(&str, &str)]) -> (ExitStatus, String) {
    let dir = tempfile::tempdir().unwrap();
    let mut command = Command::new(env!("CARGO_BIN_EXE_cassette"));
    command
        .args(["record", "--dir", dir.path().to_str().unwrap()])
        .args(["--upstream", "gh=https://api.github.com"])
        .args(["--listen", "127.0.0.1:0"])
        .env_remove("SSL_CERT_FILE")
        .stdout(Stdio::null())
        .stderr(Stdio::piped());
    for variable in PROXY_VARIABLES {
        command.env_remove(variable);
    }
    command.envs(env.iter().copied());
    let mut child = command.spawn().unwrap();
    for _ in 0..100 {
        if let Some(status) = child.try_wait().unwrap() {
            let mut stderr = String::new();
            child
                .stderr
                .take()
                .unwrap()
                .read_to_string(&mut stderr)
                .unwrap();
            return (status, stderr);
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    child.kill().unwrap();
    child.wait().unwrap();
    panic!("cassette kept running");
}

#[test]
fn an_unreadable_ssl_cert_file_is_a_startup_error() {
    let dir = tempfile::tempdir().unwrap();
    let empty = dir.path().join("empty.pem");
    std::fs::write(&empty, "not a certificate\n").unwrap();
    for file in [dir.path().join("missing.pem"), empty] {
        let (status, stderr) = startup_failure(&[("SSL_CERT_FILE", file.to_str().unwrap())]);
        assert_eq!(status.code(), Some(2), "{stderr}");
        assert!(stderr.contains("SSL_CERT_FILE"), "{stderr}");
    }
}

#[test]
fn an_https_proxy_url_is_a_startup_error_that_hides_its_credentials() {
    let (status, stderr) = startup_failure(&[(
        "HTTPS_PROXY",
        &format!("https://u:{PROXY_SECRET}@proxy:3128"),
    )]);
    assert_eq!(status.code(), Some(2), "{stderr}");
    assert!(stderr.contains("http://"), "{stderr}");
    assert!(!stderr.contains(PROXY_SECRET), "{stderr}");
}

#[test]
fn a_malformed_proxy_value_is_a_startup_error_that_hides_it() {
    for value in [
        format!("ftp://u:{PROXY_SECRET}@proxy:21"),
        format!("http://u:{PROXY_SECRET}@pro xy:3128"),
    ] {
        let (status, stderr) = startup_failure(&[("https_proxy", &value)]);
        assert_eq!(status.code(), Some(2), "{stderr}");
        assert!(stderr.contains("https_proxy"), "{stderr}");
        assert!(!stderr.contains(PROXY_SECRET), "{stderr}");
    }
}

#[tokio::test]
async fn a_tunnel_to_an_http_origin_names_port_80() {
    let work = tempfile::tempdir().unwrap();
    let proxy = connect_proxy().await;
    let proxy_url = format!("http://127.0.0.1:{}", proxy.port);
    let recorder = Recorder::start(
        work.path(),
        "plain=http://example.invalid",
        &[("HTTP_PROXY", &proxy_url)],
    );

    let (status, _) = get(&format!("{}/plain/x", recorder.base), "Bearer none").await;
    assert_eq!(status, StatusCode::BAD_GATEWAY);
    let heads = proxy.seen.lock().clone();
    assert_eq!(heads.len(), 1, "{heads:?}");
    assert!(
        heads[0].starts_with("CONNECT example.invalid:80 HTTP/1.1\r\n"),
        "{heads:?}"
    );
}
