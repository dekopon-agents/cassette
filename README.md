# cassette

Record/replay HTTP server for Dekopon provider `baseUrl`. Record a provider's real vendor traffic
once, then replay it with no network: for load tests, Harbor tasks and edited failure scenarios.

One listener serves every provider under a path prefix. A request to `/<name>/<path>` belongs to
the upstream called `<name>`, so a provider's setting is

```yaml
providerSettings:
  exa-search:
    baseUrl: http://127.0.0.1:8787/exa
```

## Record

```sh
cassette record --dir cassettes \
  --upstream exa=https://api.exa.ai \
  --upstream gh=https://api.github.com
```

Each request to `/<name>/<path>?<query>` is forwarded to `<origin>/<path>?<query>` with its method,
body and headers, minus hop-by-hop headers, `Host` and `Accept-Encoding` (so saved bodies stay
readable). Redirects are not followed. The vendor's status, headers and body go back to the caller
unchanged, and the exchange is saved as one file under `cassettes/<name>/`.

Credential-bearing request headers are saved as `"[redacted]"`: `authorization`,
`proxy-authorization`, `cookie`, `x-api-key`, `api-key`, `chatgpt-account-id`, and any header
named with `--redact <header>` (repeatable). `set-cookie` is never saved. A response that echoes
a redacted value (the whole header value, or the token after `Bearer `/`Basic `) anywhere in its
headers or body is not saved: the caller gets `502` and stderr names the cassette file, never the
value. A record session continues the numbering of files already in the directory.

## Replay

```sh
cassette replay --dir cassettes
```

Replay never opens an outbound connection. A request matches a recording on

- method,
- path, query included,
- body: equal as JSON when both sides parse as JSON (key order and whitespace do not matter),
  otherwise byte-equal,
- `Accept`: equal values, where an absent `Accept` matches only an absent one.

Other headers do not take part. Several recordings of one request are served in recorded order,
one each; once they are used up, the request is a miss. A miss answers `501` with
`cassette miss: METHOD /name/path?query`, prints the same line to stderr, and the process exits
with status 1. Startup errors exit with status 2; `SIGINT`/`SIGTERM` exit 0.

Both commands take `--listen ADDR` (default `127.0.0.1:8787`, or `CASSETTE_LISTEN`) and print
`cassette listening on http://ADDR` once the socket is bound; `--listen 127.0.0.1:0` picks a free
port.

## Cassette files

```
cassettes/
  exa/
    0001-POST-search.json
    0002-POST-search.json
  gh/
    0001-GET-repos-dekopon-agents-cassette.json
```

Files are named `NNNN-METHOD-<slug>.json`. The number is the recording order within one upstream
and is all replay reads from the name; the slug is for people. Every file is one exchange:

```json
{
  "version": 1,
  "request": {
    "method": "GET",
    "path": "/posts/1",
    "query": null,
    "headers": {
      "accept": "application/json",
      "authorization": "[redacted]"
    },
    "body": null
  },
  "response": {
    "status": 200,
    "headers": {
      "content-type": "application/json; charset=utf-8",
      "link": ["<https://example.com/?page=2>; rel=\"next\"", "<https://example.com/?page=9>; rel=\"last\""]
    },
    "body": {"json": {"id": 1, "title": "first"}}
  }
}
```

- `path` is the path after `/<name>`; `query` is the raw query string or `null`.
- `headers` maps a lowercase header name to a string, or to a list when the header repeats.
  Request headers are informational except `accept`. Response headers are served as written,
  minus hop-by-hop ones; `content-length` is computed from the body.
- `body` is `null` when empty, `{"json": <value>}` when the content type is JSON and the body
  parses, `{"text": "<utf-8>"}` for other UTF-8, and `{"base64": "<standard base64>"}` otherwise.

Hand edits are the way to stage a scenario: set `"status": 429` with
`"body": {"text": "slow down"}`, add a `retry-after` header, or write a payload the vendor never
sent. A file can be written from scratch; it needs only a number at the front of its name.

## Install

Release archives for `aarch64-apple-darwin`, `x86_64-unknown-linux-gnu` and
`aarch64-unknown-linux-gnu`, with `.sha256` sidecars and provenance attestations, are on the
[releases page](https://github.com/dekopon-agents/cassette/releases).

```sh
brew install dekopon-agents/tap/cassette
```

The container image `ghcr.io/dekopon-agents/cassette:<tag>` (linux/amd64, linux/arm64) runs
`cassette` as its entrypoint and listens on `0.0.0.0:8787`:

```sh
docker run --rm --network none -v "$PWD/cassettes:/cassettes:ro" \
  ghcr.io/dekopon-agents/cassette:v0.1.0 replay --dir /cassettes
```

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE) or
[MIT license](LICENSE-MIT) at your option.
