use std::collections::HashMap;
use std::convert::Infallible;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, Ordering};

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::header::{self, HeaderName};
use hyper::{HeaderMap, Request, Response, StatusCode, Uri, Version};
use hyper_rustls::HttpsConnector;
use hyper_util::client::legacy::Client;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::rt::TokioExecutor;

use crate::cassette::{self, Body, Cassette, Stored};
use crate::error::Error;
use crate::server::{Reply, text};

const CREDENTIAL_HEADERS: [&str; 6] = [
    "authorization",
    "proxy-authorization",
    "cookie",
    "x-api-key",
    "api-key",
    "chatgpt-account-id",
];

#[derive(Clone, Debug)]
pub struct UpstreamArg {
    pub name: String,
    pub origin: String,
}

pub fn parse_upstream(argument: &str) -> Result<UpstreamArg, String> {
    let (name, origin) = argument
        .split_once('=')
        .ok_or_else(|| "expected NAME=ORIGIN".to_owned())?;
    if !cassette::is_valid_name(name) {
        return Err(format!(
            "{name:?} is not a NAME: use letters, digits, '-', '_' and '.'"
        ));
    }
    let uri: Uri = origin
        .parse()
        .map_err(|error| format!("{origin:?} is not an origin: {error}"))?;
    if !matches!(uri.scheme_str(), Some("http" | "https")) || uri.authority().is_none() {
        return Err(format!("{origin:?} must be an http:// or https:// origin"));
    }
    if uri.query().is_some() {
        return Err(format!("{origin:?} must not carry a query"));
    }
    Ok(UpstreamArg {
        name: name.to_owned(),
        origin: origin.trim_end_matches('/').to_owned(),
    })
}

struct Upstream {
    origin: String,
    dir: PathBuf,
    next: AtomicU32,
}

pub struct Recorder {
    upstreams: HashMap<String, Upstream>,
    redact: Vec<HeaderName>,
    client: Client<HttpsConnector<HttpConnector>, Full<Bytes>>,
}

impl Recorder {
    pub fn open(
        dir: &Path,
        upstreams: Vec<UpstreamArg>,
        redact: Vec<HeaderName>,
    ) -> Result<Recorder, Error> {
        let mut seen = HashMap::new();
        for upstream in &upstreams {
            *seen.entry(upstream.name.as_str()).or_insert(0) += 1;
        }
        let mut duplicates: Vec<String> = seen
            .into_iter()
            .filter(|(_, count)| *count > 1)
            .map(|(name, _)| name.to_owned())
            .collect();
        if !duplicates.is_empty() {
            duplicates.sort();
            return Err(Error::DuplicateUpstreams(duplicates));
        }

        let mut by_name = HashMap::new();
        for UpstreamArg { name, origin } in upstreams {
            let dir = dir.join(&name);
            std::fs::create_dir_all(&dir).map_err(|source| Error::Io {
                path: dir.clone(),
                source,
            })?;
            let next = next_sequence(&dir)?;
            by_name.insert(
                name,
                Upstream {
                    origin,
                    dir,
                    next: AtomicU32::new(next),
                },
            );
        }

        let mut redact_all: Vec<HeaderName> = CREDENTIAL_HEADERS
            .iter()
            .map(|name| HeaderName::from_static(name))
            .collect();
        redact_all.extend(redact);

        let connector = hyper_rustls::HttpsConnectorBuilder::new()
            .with_webpki_roots()
            .https_or_http()
            .enable_http1()
            .build();
        Ok(Recorder {
            upstreams: by_name,
            redact: redact_all,
            client: Client::builder(TokioExecutor::new()).build(connector),
        })
    }

    pub async fn handle(&self, request: Request<Incoming>) -> Result<Reply, Infallible> {
        let (name, path) = cassette::route(request.uri().path());
        let Some(upstream) = self.upstreams.get(name) else {
            return Ok(text(
                StatusCode::NOT_FOUND,
                format!("cassette: no upstream named {name:?}\n"),
            ));
        };
        let sequence = upstream.next.fetch_add(1, Ordering::Relaxed);
        let path = path.to_owned();
        let query = request.uri().query().map(str::to_owned);

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

        let target = match &query {
            Some(query) => format!("{}{path}?{query}", upstream.origin),
            None => format!("{}{path}", upstream.origin),
        };
        let mut outbound = Request::new(Full::new(body.clone()));
        *outbound.method_mut() = parts.method.clone();
        *outbound.version_mut() = Version::HTTP_11;
        match target.parse() {
            Ok(uri) => *outbound.uri_mut() = uri,
            Err(error) => {
                return Ok(text(
                    StatusCode::BAD_REQUEST,
                    format!("cassette: {target:?} is not a URL: {error}\n"),
                ));
            }
        }
        for (header_name, value) in &parts.headers {
            if forwarded(header_name, &parts.headers) {
                outbound
                    .headers_mut()
                    .append(header_name.clone(), value.clone());
            }
        }

        let response = match self.client.request(outbound).await {
            Ok(response) => response,
            Err(error) => {
                eprintln!("cassette upstream {target}: {error}");
                return Ok(text(
                    StatusCode::BAD_GATEWAY,
                    format!("cassette: upstream {target}: {error}\n"),
                ));
            }
        };
        let (response_parts, response_body) = response.into_parts();
        let response_body = match response_body.collect().await {
            Ok(collected) => collected.to_bytes(),
            Err(error) => {
                eprintln!("cassette upstream {target}: {error}");
                return Ok(text(
                    StatusCode::BAD_GATEWAY,
                    format!("cassette: upstream {target}: {error}\n"),
                ));
            }
        };

        let cassette = Cassette {
            version: cassette::VERSION,
            request: cassette::Request {
                method: parts.method.as_str().to_owned(),
                path: path.clone(),
                query,
                headers: cassette::stored_headers(&parts.headers, |header_name| {
                    if self.redact.contains(header_name) {
                        Stored::Redacted
                    } else {
                        Stored::Value
                    }
                }),
                body: Body::encode(&parts.headers, &body),
            },
            response: cassette::Response {
                status: response_parts.status.as_u16(),
                headers: cassette::stored_headers(&response_parts.headers, |header_name| {
                    if *header_name == header::SET_COOKIE
                        || *header_name == header::CONTENT_LENGTH
                        || cassette::is_hop_by_hop(header_name, &response_parts.headers)
                    {
                        Stored::Dropped
                    } else {
                        Stored::Value
                    }
                }),
                body: Body::encode(&response_parts.headers, &response_body),
            },
        };
        let file = upstream
            .dir
            .join(cassette::file_name(sequence, parts.method.as_str(), &path));
        let secrets: Vec<&[u8]> = parts
            .headers
            .iter()
            .filter(|(header_name, _)| self.redact.contains(header_name))
            .flat_map(|(_, value)| secret_forms(value.as_bytes()))
            .collect();
        let raw_echo = echoes(
            &secrets,
            response_parts
                .headers
                .iter()
                .flat_map(|(header_name, value)| {
                    [header_name.as_str().as_bytes(), value.as_bytes()]
                })
                .chain([&response_body[..]]),
        );
        let saved = if raw_echo {
            Err(Error::Echoed { path: file })
        } else {
            save(&file, &cassette, &secrets).await
        };
        if let Err(error) = saved {
            eprintln!("cassette: {error}");
            let status = match error {
                Error::Echoed { .. } => StatusCode::BAD_GATEWAY,
                _ => StatusCode::INTERNAL_SERVER_ERROR,
            };
            return Ok(text(status, format!("cassette: {error}\n")));
        }

        let mut reply = Response::new(Full::new(response_body));
        *reply.status_mut() = response_parts.status;
        copy_response_headers(&response_parts.headers, reply.headers_mut());
        Ok(reply)
    }
}

fn forwarded(name: &HeaderName, headers: &HeaderMap) -> bool {
    !cassette::is_hop_by_hop(name, headers)
        && *name != header::HOST
        && *name != header::CONTENT_LENGTH
        && *name != header::ACCEPT_ENCODING
}

pub fn copy_response_headers(from: &HeaderMap, to: &mut HeaderMap) {
    for (name, value) in from {
        if !cassette::is_hop_by_hop(name, from) && *name != header::CONTENT_LENGTH {
            to.append(name.clone(), value.clone());
        }
    }
}

fn next_sequence(dir: &Path) -> Result<u32, Error> {
    let entries = std::fs::read_dir(dir).map_err(|source| Error::Io {
        path: dir.to_owned(),
        source,
    })?;
    let mut last = 0;
    for entry in entries {
        let entry = entry.map_err(|source| Error::Io {
            path: dir.to_owned(),
            source,
        })?;
        if let Some(sequence) = entry.file_name().to_str().and_then(cassette::sequence_of) {
            last = last.max(sequence);
        }
    }
    Ok(last + 1)
}

fn secret_forms(value: &[u8]) -> Vec<&[u8]> {
    let mut forms = vec![value];
    for scheme in [&b"bearer "[..], b"basic "] {
        if let Some((head, token)) = value.split_at_checked(scheme.len())
            && head.eq_ignore_ascii_case(scheme)
        {
            forms.push(token.trim_ascii());
        }
    }
    forms.retain(|form| !form.is_empty());
    forms
}

fn echoes<'a>(secrets: &[&[u8]], haystacks: impl IntoIterator<Item = &'a [u8]>) -> bool {
    haystacks.into_iter().any(|haystack| {
        secrets.iter().any(|secret| {
            haystack
                .windows(secret.len())
                .any(|window| window == *secret)
        })
    })
}

async fn save(file: &Path, cassette: &Cassette, secrets: &[&[u8]]) -> Result<(), Error> {
    let mut json = serde_json::to_vec_pretty(cassette).map_err(|source| Error::Parse {
        path: file.to_owned(),
        source,
    })?;
    if echoes(secrets, [&json[..]]) {
        return Err(Error::Echoed {
            path: file.to_owned(),
        });
    }
    json.push(b'\n');
    let mut temporary = file.as_os_str().to_owned();
    temporary.push(".tmp");
    let temporary = PathBuf::from(temporary);
    tokio::fs::write(&temporary, json)
        .await
        .map_err(|source| Error::Io {
            path: temporary.clone(),
            source,
        })?;
    tokio::fs::rename(&temporary, file)
        .await
        .map_err(|source| Error::Io {
            path: file.to_owned(),
            source,
        })
}
