use std::collections::{HashMap, VecDeque};
use std::convert::Infallible;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::header::{self, HeaderName, HeaderValue};
use hyper::{HeaderMap, Request, Response, StatusCode};
use parking_lot::Mutex;
use serde_json::Value;
use tokio::sync::Notify;

use crate::cassette::{self, Cassette};
use crate::error::Error;
use crate::record::copy_response_headers;
use crate::server::{Reply, text};

#[derive(Debug, Hash, PartialEq, Eq)]
struct Key {
    name: String,
    method: String,
    target: String,
    accept: Option<String>,
    body: BodyKey,
}

#[derive(Debug, Hash, PartialEq, Eq)]
enum BodyKey {
    Json(String),
    Bytes(Bytes),
}

impl BodyKey {
    fn of(bytes: Bytes) -> BodyKey {
        match serde_json::from_slice::<Value>(&bytes) {
            Ok(value) => BodyKey::Json(canonical(&value).to_string()),
            Err(_) => BodyKey::Bytes(bytes),
        }
    }
}

fn canonical(value: &Value) -> Value {
    match value {
        Value::Object(map) => {
            let mut entries: Vec<(&String, &Value)> = map.iter().collect();
            entries.sort_by(|left, right| left.0.cmp(right.0));
            Value::Object(
                entries
                    .into_iter()
                    .map(|(name, value)| (name.clone(), canonical(value)))
                    .collect(),
            )
        }
        Value::Array(items) => Value::Array(items.iter().map(canonical).collect()),
        other => other.clone(),
    }
}

fn target(path: &str, query: Option<&str>) -> String {
    match query {
        Some(query) => format!("{path}?{query}"),
        None => path.to_owned(),
    }
}

struct Recorded {
    status: StatusCode,
    headers: HeaderMap,
    body: Bytes,
}

pub struct Tape {
    recordings: Mutex<HashMap<Key, VecDeque<Recorded>>>,
    missed: AtomicBool,
    stop: Notify,
}

impl Tape {
    pub fn load(dir: &Path) -> Result<Tape, Error> {
        let mut recordings: HashMap<Key, VecDeque<Recorded>> = HashMap::new();
        for (name, provider_dir) in sorted_entries(dir, |entry| entry.is_dir())? {
            if !cassette::is_valid_name(&name) {
                continue;
            }
            let mut files: Vec<(u32, String, PathBuf)> = Vec::new();
            for (file_name, path) in sorted_entries(&provider_dir, |entry| entry.is_file())? {
                if file_name.starts_with('.') || !file_name.ends_with(".json") {
                    continue;
                }
                let sequence = cassette::sequence_of(&file_name).ok_or_else(|| Error::Invalid {
                    path: path.clone(),
                    reason: "a cassette file name starts with its sequence number, as in 0001-GET-items.json"
                        .to_owned(),
                })?;
                files.push((sequence, file_name, path));
            }
            files.sort();
            for (_, _, path) in files {
                let (key, recorded) = read(&name, &path)?;
                recordings.entry(key).or_default().push_back(recorded);
            }
        }
        Ok(Tape {
            recordings: Mutex::new(recordings),
            missed: AtomicBool::new(false),
            stop: Notify::new(),
        })
    }

    pub fn missed(&self) -> bool {
        self.missed.load(Ordering::SeqCst)
    }

    pub async fn stopped(&self) {
        self.stop.notified().await;
    }

    pub async fn handle(&self, request: Request<Incoming>) -> Result<Reply, Infallible> {
        let (parts, body) = request.into_parts();
        let body = match body.collect().await {
            Ok(collected) => collected.to_bytes(),
            Err(error) => {
                return Ok(text(
                    StatusCode::BAD_REQUEST,
                    format!("cassette: read request body: {error}\n"),
                ));
            }
        };
        let (name, path) = cassette::route(parts.uri.path());
        let key = Key {
            name: name.to_owned(),
            method: parts.method.as_str().to_owned(),
            target: target(path, parts.uri.query()),
            accept: accept(
                parts
                    .headers
                    .get_all(header::ACCEPT)
                    .iter()
                    .map(|value| String::from_utf8_lossy(value.as_bytes()).into_owned()),
            ),
            body: BodyKey::of(body),
        };
        let recorded = self
            .recordings
            .lock()
            .get_mut(&key)
            .and_then(VecDeque::pop_front);
        match recorded {
            Some(recorded) => {
                let mut reply = Response::new(Full::new(recorded.body));
                *reply.status_mut() = recorded.status;
                *reply.headers_mut() = recorded.headers;
                Ok(reply)
            }
            None => {
                let line = format!(
                    "cassette miss: {} {}",
                    parts.method,
                    parts
                        .uri
                        .path_and_query()
                        .map_or(parts.uri.path(), |target| target.as_str())
                );
                eprintln!("{line}");
                self.missed.store(true, Ordering::SeqCst);
                self.stop.notify_one();
                Ok(text(StatusCode::NOT_IMPLEMENTED, format!("{line}\n")))
            }
        }
    }
}

fn accept(values: impl Iterator<Item = String>) -> Option<String> {
    let values: Vec<String> = values.collect();
    if values.is_empty() {
        None
    } else {
        Some(values.join(", "))
    }
}

fn sorted_entries(
    dir: &Path,
    keep: impl Fn(&Path) -> bool,
) -> Result<Vec<(String, PathBuf)>, Error> {
    let io = |source| Error::Io {
        path: dir.to_owned(),
        source,
    };
    let mut entries = Vec::new();
    for entry in std::fs::read_dir(dir).map_err(io)? {
        let path = entry.map_err(io)?.path();
        if !keep(&path) {
            continue;
        }
        if let Some(name) = path.file_name().and_then(|name| name.to_str()) {
            entries.push((name.to_owned(), path.clone()));
        }
    }
    entries.sort();
    Ok(entries)
}

fn read(name: &str, path: &Path) -> Result<(Key, Recorded), Error> {
    let invalid = |reason: String| Error::Invalid {
        path: path.to_owned(),
        reason,
    };
    let bytes = std::fs::read(path).map_err(|source| Error::Io {
        path: path.to_owned(),
        source,
    })?;
    let cassette: Cassette = serde_json::from_slice(&bytes).map_err(|source| Error::Parse {
        path: path.to_owned(),
        source,
    })?;
    if cassette.version != cassette::VERSION {
        return Err(invalid(format!(
            "version {} is not understood; this cassette reads version {}",
            cassette.version,
            cassette::VERSION
        )));
    }
    let request = cassette.request;
    let request_body = match &request.body {
        Some(body) => body
            .decode()
            .map_err(|error| invalid(format!("request body: {error}")))?,
        None => Bytes::new(),
    };
    let accept = accept(
        request
            .headers
            .iter()
            .filter(|(header_name, _)| header_name.eq_ignore_ascii_case("accept"))
            .flat_map(|(_, values)| values.iter().cloned()),
    );
    let key = Key {
        name: name.to_owned(),
        method: request.method,
        target: target(&request.path, request.query.as_deref()),
        accept,
        body: BodyKey::of(request_body),
    };

    let response = cassette.response;
    let status = StatusCode::from_u16(response.status)
        .map_err(|error| invalid(format!("status: {error}")))?;
    let mut stored = HeaderMap::new();
    for (header_name, values) in &response.headers {
        let header_name = HeaderName::from_bytes(header_name.as_bytes())
            .map_err(|error| invalid(format!("response header {header_name:?}: {error}")))?;
        for value in values.iter() {
            let value = HeaderValue::from_str(value)
                .map_err(|error| invalid(format!("response header value {value:?}: {error}")))?;
            stored.append(header_name.clone(), value);
        }
    }
    let mut headers = HeaderMap::new();
    copy_response_headers(&stored, &mut headers);
    let body = match &response.body {
        Some(body) => body
            .decode()
            .map_err(|error| invalid(format!("response body: {error}")))?,
        None => Bytes::new(),
    };
    Ok((
        key,
        Recorded {
            status,
            headers,
            body,
        },
    ))
}
