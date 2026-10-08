use std::convert::Infallible;
use std::io::{BufRead, BufReader, Read};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::header::{self, HeaderValue};
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{HeaderMap, Method, Request, Response, StatusCode};
use hyper_util::client::legacy::Client;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::rt::{TokioExecutor, TokioIo};
use parking_lot::Mutex;
use serde_json::{Value, json};
use tokio::net::TcpListener;
use tokio::task::JoinHandle;

const SECRET: &str = "sk-synthetic-4f1c9a7e2b";
const CUSTOM_SECRET: &str = "custom-synthetic-77d0";

struct Upstream {
    base: String,
    authorizations: Arc<Mutex<Vec<String>>>,
    task: JoinHandle<()>,
}

impl Drop for Upstream {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn upstream() -> Upstream {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let authorizations = Arc::new(Mutex::new(Vec::new()));
    let counter = Arc::new(AtomicU32::new(0));
    let seen = Arc::clone(&authorizations);
    let task = tokio::spawn(async move {
        loop {
            let (stream, _) = listener.accept().await.unwrap();
            let seen = Arc::clone(&seen);
            let counter = Arc::clone(&counter);
            tokio::spawn(async move {
                let service = service_fn(move |request: Request<Incoming>| {
                    let seen = Arc::clone(&seen);
                    let counter = Arc::clone(&counter);
                    async move { Ok::<_, Infallible>(respond(request, &seen, &counter).await) }
                });
                http1::Builder::new()
                    .serve_connection(TokioIo::new(stream), service)
                    .await
                    .unwrap();
            });
        }
    });
    Upstream {
        base,
        authorizations,
        task,
    }
}

async fn respond(
    request: Request<Incoming>,
    seen: &Mutex<Vec<String>>,
    counter: &AtomicU32,
) -> Response<Full<Bytes>> {
    if let Some(value) = request.headers().get(header::AUTHORIZATION) {
        seen.lock().push(value.to_str().unwrap().to_owned());
    }
    let echoed = request.headers().get(header::AUTHORIZATION).cloned();
    let accept = request
        .headers()
        .get(header::ACCEPT)
        .map(|value| value.to_str().unwrap().to_owned());
    let method = request.method().clone();
    let path = request.uri().path().to_owned();
    let body = request.into_body().collect().await.unwrap().to_bytes();
    let (status, content_type, body) = match (method, path.as_str()) {
        (Method::GET, "/items") => (
            StatusCode::OK,
            "application/json; charset=utf-8",
            Bytes::from_static(br#"{"page": "2", "items": [1, 2]}"#),
        ),
        (Method::POST, "/items") => {
            let created: Value = serde_json::from_slice(&body).unwrap();
            (
                StatusCode::CREATED,
                "application/json",
                Bytes::from(json!({"created": created}).to_string()),
            )
        }
        (Method::GET, "/counter") => (
            StatusCode::OK,
            "application/json",
            Bytes::from(json!({"n": counter.fetch_add(1, Ordering::SeqCst) + 1}).to_string()),
        ),
        (Method::GET, "/pulls/1") if accept.as_deref() == Some("application/vnd.github.diff") => (
            StatusCode::OK,
            "text/x-diff",
            Bytes::from_static(b"diff --git a/x b/x\n"),
        ),
        (Method::GET, "/pulls/1") => (
            StatusCode::OK,
            "application/json",
            Bytes::from_static(br#"{"number": 1}"#),
        ),
        (Method::GET, "/echo") => {
            let echoed = echoed.unwrap();
            let mut response = Response::new(Full::new(Bytes::from(
                json!({"you sent": echoed.to_str().unwrap()}).to_string(),
            )));
            response.headers_mut().insert("x-echo", echoed);
            return response;
        }
        (Method::GET, "/binary") => (
            StatusCode::OK,
            "application/octet-stream",
            Bytes::from_static(&[0xff, 0x00, 0xfe, 0x80]),
        ),
        _ => (StatusCode::NOT_FOUND, "text/plain", Bytes::new()),
    };
    let mut response = Response::new(Full::new(body));
    *response.status_mut() = status;
    response
        .headers_mut()
        .insert(header::CONTENT_TYPE, HeaderValue::from_static(content_type));
    response.headers_mut().insert(
        header::SET_COOKIE,
        HeaderValue::from_static("session=upstream-cookie"),
    );
    response
}

struct Cassette {
    child: Child,
    base: String,
}

impl Cassette {
    fn start(arguments: &[&str]) -> Cassette {
        let mut child = Command::new(env!("CARGO_BIN_EXE_cassette"))
            .args(arguments)
            .args(["--listen", "127.0.0.1:0"])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let mut line = String::new();
        BufReader::new(child.stdout.take().unwrap())
            .read_line(&mut line)
            .unwrap();
        let base = line
            .trim()
            .strip_prefix("cassette listening on ")
            .unwrap_or_else(|| panic!("unexpected first line {line:?}"))
            .to_owned();
        Cassette { child, base }
    }

    fn record(dir: &Path, upstream: &Upstream, extra: &[&str]) -> Cassette {
        let upstream = format!("api={}", upstream.base);
        let mut arguments = vec!["record", "--dir", dir.to_str().unwrap(), "--upstream"];
        arguments.push(&upstream);
        arguments.extend(extra);
        Cassette::start(&arguments)
    }

    fn replay(dir: &Path) -> Cassette {
        Cassette::start(&["replay", "--dir", dir.to_str().unwrap()])
    }

    async fn exit(mut self) -> (ExitStatus, String) {
        for _ in 0..200 {
            if let Some(status) = self.child.try_wait().unwrap() {
                let mut stderr = String::new();
                self.child
                    .stderr
                    .take()
                    .unwrap()
                    .read_to_string(&mut stderr)
                    .unwrap();
                return (status, stderr);
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        panic!("cassette did not exit");
    }
}

impl Drop for Cassette {
    fn drop(&mut self) {
        if let Err(error) = self.child.kill() {
            eprintln!("kill cassette: {error}");
        }
        if let Err(error) = self.child.wait() {
            eprintln!("wait for cassette: {error}");
        }
    }
}

struct Reply {
    status: StatusCode,
    headers: HeaderMap,
    body: Bytes,
}

impl Reply {
    fn json(&self) -> Value {
        serde_json::from_slice(&self.body).unwrap()
    }
}

async fn send(method: Method, url: &str, headers: &[(&str, &str)], body: &str) -> Reply {
    let client: Client<HttpConnector, Full<Bytes>> =
        Client::builder(TokioExecutor::new()).build_http();
    let mut request = Request::new(Full::new(Bytes::copy_from_slice(body.as_bytes())));
    *request.method_mut() = method;
    *request.uri_mut() = url.parse().unwrap();
    for (name, value) in headers {
        request.headers_mut().append(
            header::HeaderName::from_bytes(name.as_bytes()).unwrap(),
            HeaderValue::from_str(value).unwrap(),
        );
    }
    let response = client.request(request).await.unwrap();
    let status = response.status();
    let headers = response.headers().clone();
    let body = response.into_body().collect().await.unwrap().to_bytes();
    Reply {
        status,
        headers,
        body,
    }
}

async fn get(url: &str, headers: &[(&str, &str)]) -> Reply {
    send(Method::GET, url, headers, "").await
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

fn names(dir: &Path) -> Vec<String> {
    files(dir)
        .iter()
        .map(|path| path.strip_prefix(dir).unwrap().to_str().unwrap().to_owned())
        .collect()
}

fn read_json(path: &Path) -> Value {
    serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap()
}

#[tokio::test]
async fn recording_forwards_and_saves_each_exchange_with_credentials_redacted() {
    let dir = tempfile::tempdir().unwrap();
    let upstream = upstream().await;
    let recorder = Cassette::record(dir.path(), &upstream, &["--redact", "x-custom-secret"]);
    let bearer = format!("Bearer {SECRET}");

    let reply = get(
        &format!("{}/api/items?page=2", recorder.base),
        &[
            ("authorization", &bearer),
            ("x-api-key", SECRET),
            ("x-custom-secret", CUSTOM_SECRET),
            ("accept", "application/json"),
        ],
    )
    .await;
    assert_eq!(reply.status, StatusCode::OK);
    assert_eq!(&reply.body[..], br#"{"page": "2", "items": [1, 2]}"#);
    assert_eq!(reply.headers[header::SET_COOKIE], "session=upstream-cookie");
    assert_eq!(*upstream.authorizations.lock(), vec![bearer.clone()]);

    let created = send(
        Method::POST,
        &format!("{}/api/items", recorder.base),
        &[
            ("authorization", &bearer),
            ("content-type", "application/json"),
        ],
        r#"{"title": "one"}"#,
    )
    .await;
    assert_eq!(created.status, StatusCode::CREATED);

    assert_eq!(
        names(dir.path()),
        ["api/0001-GET-items.json", "api/0002-POST-items.json"]
    );
    let saved = read_json(&dir.path().join("api/0001-GET-items.json"));
    assert_eq!(
        saved,
        json!({
            "version": 1,
            "request": {
                "method": "GET",
                "path": "/items",
                "query": "page=2",
                "headers": {
                    "accept": "application/json",
                    "authorization": "[redacted]",
                    "host": recorder.base.strip_prefix("http://").unwrap(),
                    "x-api-key": "[redacted]",
                    "x-custom-secret": "[redacted]",
                },
                "body": null,
            },
            "response": {
                "status": 200,
                "headers": {
                    "content-type": "application/json; charset=utf-8",
                    "date": saved["response"]["headers"]["date"],
                },
                "body": {"json": {"page": "2", "items": [1, 2]}},
            },
        })
    );
    let posted = read_json(&dir.path().join("api/0002-POST-items.json"));
    assert_eq!(posted["request"]["body"], json!({"json": {"title": "one"}}));

    for file in files(dir.path()) {
        let bytes = std::fs::read(&file).unwrap();
        for secret in [SECRET, CUSTOM_SECRET] {
            assert!(
                !bytes
                    .windows(secret.len())
                    .any(|window| window == secret.as_bytes()),
                "{} holds a credential",
                file.display()
            );
        }
    }
}

#[tokio::test]
async fn replay_serves_recordings_with_no_upstream() {
    let dir = tempfile::tempdir().unwrap();
    {
        let upstream = upstream().await;
        let recorder = Cassette::record(dir.path(), &upstream, &[]);
        get(&format!("{}/api/items?page=2", recorder.base), &[]).await;
        send(
            Method::POST,
            &format!("{}/api/items", recorder.base),
            &[("content-type", "application/json")],
            r#"{"title": "one", "tags": ["a", "b"]}"#,
        )
        .await;
        get(&format!("{}/api/binary", recorder.base), &[]).await;
    }

    let replay = Cassette::replay(dir.path());
    let items = get(&format!("{}/api/items?page=2", replay.base), &[]).await;
    assert_eq!(items.status, StatusCode::OK);
    assert_eq!(items.json(), json!({"page": "2", "items": [1, 2]}));
    assert_eq!(
        items.headers[header::CONTENT_TYPE],
        "application/json; charset=utf-8"
    );
    assert!(!items.headers.contains_key(header::SET_COOKIE));

    let created = send(
        Method::POST,
        &format!("{}/api/items", replay.base),
        &[],
        r#"{ "tags": ["a", "b"], "title": "one" }"#,
    )
    .await;
    assert_eq!(created.status, StatusCode::CREATED);
    assert_eq!(
        created.json(),
        json!({"created": {"title": "one", "tags": ["a", "b"]}})
    );

    let binary = get(&format!("{}/api/binary", replay.base), &[]).await;
    assert_eq!(&binary.body[..], &[0xff, 0x00, 0xfe, 0x80]);
}

#[tokio::test]
async fn an_edited_status_is_served() {
    let dir = tempfile::tempdir().unwrap();
    {
        let upstream = upstream().await;
        let recorder = Cassette::record(dir.path(), &upstream, &[]);
        get(&format!("{}/api/items?page=2", recorder.base), &[]).await;
    }
    let file = dir.path().join("api/0001-GET-items.json");
    let mut cassette = read_json(&file);
    cassette["response"]["status"] = json!(429);
    cassette["response"]["body"] = json!({"text": "slow down"});
    std::fs::write(&file, serde_json::to_vec_pretty(&cassette).unwrap()).unwrap();

    let replay = Cassette::replay(dir.path());
    let reply = get(&format!("{}/api/items?page=2", replay.base), &[]).await;
    assert_eq!(reply.status, StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(&reply.body[..], b"slow down");
}

#[tokio::test]
async fn a_miss_answers_501_and_exits_non_zero_naming_method_and_path() {
    let dir = tempfile::tempdir().unwrap();
    let replay = Cassette::replay(dir.path());
    let reply = send(
        Method::POST,
        &format!("{}/api/nothing?x=1", replay.base),
        &[],
        "{}",
    )
    .await;
    assert_eq!(reply.status, StatusCode::NOT_IMPLEMENTED);
    assert_eq!(&reply.body[..], b"cassette miss: POST /api/nothing?x=1\n");

    let (status, stderr) = replay.exit().await;
    assert!(!status.success());
    assert!(stderr.contains("cassette miss: POST /api/nothing?x=1\n"));
}

#[tokio::test]
async fn repeats_replay_in_recorded_order_and_an_exhausted_key_misses() {
    let dir = tempfile::tempdir().unwrap();
    {
        let upstream = upstream().await;
        let recorder = Cassette::record(dir.path(), &upstream, &[]);
        for _ in 0..3 {
            get(&format!("{}/api/counter", recorder.base), &[]).await;
        }
    }

    let replay = Cassette::replay(dir.path());
    for n in 1..=3 {
        let reply = get(&format!("{}/api/counter", replay.base), &[]).await;
        assert_eq!(reply.json(), json!({"n": n}));
    }
    let exhausted = get(&format!("{}/api/counter", replay.base), &[]).await;
    assert_eq!(exhausted.status, StatusCode::NOT_IMPLEMENTED);
    let (status, stderr) = replay.exit().await;
    assert!(!status.success());
    assert!(stderr.contains("cassette miss: GET /api/counter\n"));
}

#[tokio::test]
async fn accept_is_part_of_the_match_key() {
    let dir = tempfile::tempdir().unwrap();
    let diff = [("accept", "application/vnd.github.diff")];
    let json_accept = [("accept", "application/json")];
    {
        let upstream = upstream().await;
        let recorder = Cassette::record(dir.path(), &upstream, &[]);
        get(&format!("{}/api/pulls/1", recorder.base), &diff).await;
        get(&format!("{}/api/pulls/1", recorder.base), &json_accept).await;
    }

    let replay = Cassette::replay(dir.path());
    let json_reply = get(&format!("{}/api/pulls/1", replay.base), &json_accept).await;
    assert_eq!(json_reply.json(), json!({"number": 1}));
    let diff_reply = get(&format!("{}/api/pulls/1", replay.base), &diff).await;
    assert_eq!(&diff_reply.body[..], b"diff --git a/x b/x\n");
    let bare = get(&format!("{}/api/pulls/1", replay.base), &[]).await;
    assert_eq!(bare.status, StatusCode::NOT_IMPLEMENTED);
    let (status, _) = replay.exit().await;
    assert!(!status.success());
}

#[tokio::test]
async fn a_response_echoing_a_redacted_credential_is_refused_and_not_saved() {
    let dir = tempfile::tempdir().unwrap();
    let upstream = upstream().await;
    let recorder = Cassette::record(dir.path(), &upstream, &[]);
    let bearer = format!("Bearer {SECRET}");

    let reply = get(
        &format!("{}/api/echo", recorder.base),
        &[("authorization", &bearer)],
    )
    .await;
    assert_eq!(reply.status, StatusCode::BAD_GATEWAY);
    assert!(
        !reply
            .body
            .windows(SECRET.len())
            .any(|window| window == SECRET.as_bytes())
    );
    assert!(!reply.headers.contains_key("x-echo"));
    assert_eq!(names(dir.path()), Vec::<String>::new());
}
