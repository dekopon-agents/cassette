use std::collections::BTreeMap;

use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use bytes::Bytes;
use hyper::HeaderMap;
use hyper::header::{self, HeaderName};
use serde::{Deserialize, Serialize};
use serde_json::Value;

pub const VERSION: u32 = 1;
pub const REDACTED: &str = "[redacted]";

pub type Headers = BTreeMap<String, Values>;

#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Values {
    One(String),
    Many(Vec<String>),
}

impl Values {
    pub fn iter(&self) -> std::slice::Iter<'_, String> {
        match self {
            Values::One(value) => std::slice::from_ref(value).iter(),
            Values::Many(values) => values.iter(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Cassette {
    pub version: u32,
    pub request: Request,
    pub response: Response,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Request {
    pub method: String,
    pub path: String,
    pub query: Option<String>,
    #[serde(default)]
    pub headers: Headers,
    pub body: Option<Body>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Response {
    pub status: u16,
    #[serde(default)]
    pub headers: Headers,
    pub body: Option<Body>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Body {
    Json(Value),
    Text(String),
    Base64(String),
}

impl Body {
    pub fn encode(headers: &HeaderMap, bytes: &[u8]) -> Option<Body> {
        if bytes.is_empty() {
            return None;
        }
        if is_json(headers)
            && let Ok(value) = serde_json::from_slice(bytes)
        {
            return Some(Body::Json(value));
        }
        Some(match std::str::from_utf8(bytes) {
            Ok(text) => Body::Text(text.to_owned()),
            Err(_) => Body::Base64(STANDARD.encode(bytes)),
        })
    }

    pub fn decode(&self) -> Result<Bytes, base64::DecodeError> {
        Ok(match self {
            Body::Json(value) => Bytes::from(value.to_string()),
            Body::Text(text) => Bytes::copy_from_slice(text.as_bytes()),
            Body::Base64(encoded) => Bytes::from(STANDARD.decode(encoded)?),
        })
    }
}

fn is_json(headers: &HeaderMap) -> bool {
    let Some(value) = headers.get(header::CONTENT_TYPE) else {
        return false;
    };
    let Ok(value) = value.to_str() else {
        return false;
    };
    let essence = value.split(';').next().unwrap_or_default().trim();
    let essence = essence.to_ascii_lowercase();
    essence.ends_with("/json") || essence.ends_with("+json")
}

pub fn is_hop_by_hop(name: &HeaderName, headers: &HeaderMap) -> bool {
    matches!(
        *name,
        header::CONNECTION
            | header::PROXY_AUTHENTICATE
            | header::PROXY_AUTHORIZATION
            | header::TE
            | header::TRAILER
            | header::TRANSFER_ENCODING
            | header::UPGRADE
    ) || name.as_str() == "keep-alive"
        || headers
            .get_all(header::CONNECTION)
            .iter()
            .filter_map(|value| value.to_str().ok())
            .flat_map(|value| value.split(','))
            .any(|token| token.trim().eq_ignore_ascii_case(name.as_str()))
}

pub fn stored_headers(headers: &HeaderMap, value_of: impl Fn(&HeaderName) -> Stored) -> Headers {
    let mut stored: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for (name, value) in headers {
        let value = match value_of(name) {
            Stored::Value => String::from_utf8_lossy(value.as_bytes()).into_owned(),
            Stored::Redacted => REDACTED.to_owned(),
            Stored::Dropped => continue,
        };
        stored
            .entry(name.as_str().to_owned())
            .or_default()
            .push(value);
    }
    stored
        .into_iter()
        .map(|(name, mut values)| {
            let values = if values.len() == 1 {
                Values::One(values.remove(0))
            } else {
                Values::Many(values)
            };
            (name, values)
        })
        .collect()
}

pub enum Stored {
    Value,
    Redacted,
    Dropped,
}

pub fn file_name(sequence: u32, method: &str, path: &str) -> String {
    format!("{sequence:04}-{}-{}.json", slug(method), slug(path))
}

pub fn sequence_of(file_name: &str) -> Option<u32> {
    let (digits, _) = file_name.split_once('-')?;
    if digits.is_empty() || !digits.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    digits.parse().ok()
}

fn slug(text: &str) -> String {
    let mut slug = String::new();
    for character in text.chars() {
        if character.is_ascii_alphanumeric() {
            slug.push(character);
        } else if !slug.is_empty() && !slug.ends_with('-') {
            slug.push('-');
        }
        if slug.len() >= 48 {
            break;
        }
    }
    let slug = slug.trim_end_matches('-');
    if slug.is_empty() {
        "root".to_owned()
    } else {
        slug.to_owned()
    }
}

pub fn is_valid_name(name: &str) -> bool {
    !name.is_empty()
        && name != "."
        && name != ".."
        && name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
}

pub fn route(path: &str) -> (&str, &str) {
    let path = path.strip_prefix('/').unwrap_or(path);
    match path.find('/') {
        Some(slash) => path.split_at(slash),
        None => (path, "/"),
    }
}
