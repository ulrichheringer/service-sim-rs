# service-sim-rs

Small, deterministic HTTP service simulation for Rust integration tests.
Run a real loopback service with exact request matching, scripted retries,
latency, stalled responses, malformed bytes, and TCP resets. No external process,
Docker service, or HTTP client dependency is required.

## Installation

Requires Rust 1.85+ and Tokio. This initial release is distributed from source:

```toml
[dev-dependencies]
service-sim-rs = { git = "https://github.com/ulrichheringer/service-sim-rs", branch = "main" }
tokio = { version = "1", features = ["macros", "rt", "time"] }
```

For reproducible CI, replace the branch with a pinned commit `rev`.
For a local checkout, use `service-sim-rs = { path = "../service-sim-rs" }`.
The Rust import is `service_sim_rs`.

## Quick start

```rust,no_run
use service_sim_rs::{Action, Rule, Server};

#[tokio::main(flavor = "current_thread")]
async fn main() -> std::io::Result<()> {
    let server = Server::start(vec![
        Rule::new("GET", "/health", Action::respond(503, "retry"))
            .then(Action::respond(200, "ready")),
    ]).await?;

    // Point your application's HTTP client at this URL.
    println!("{}", server.url());
    assert_eq!(server.hits(0), Some(0));
    server.shutdown().await?;
    Ok(())
}
```

Run a complete example that sends real requests and checks both replies:

```sh
cargo run --example retry
```

## Match requests and set response headers

Rules match an exact, case-sensitive method and the entire request target,
including query strings. Optional bodies match bytes exactly. The first matching
rule wins; unmatched requests receive 404. `hits(index)` counts only successful
matches and uses the rule's zero-based position. Invalid indices return `None`.

```rust,no_run
use service_sim_rs::{Action, Rule};
# fn main() -> std::io::Result<()> {
let reply = Action::respond(201, br#"{"id":42}"#.to_vec())
    .with_header("Content-Type", "application/json")?
    .with_header("Location", "/items/42")?;
let rule = Rule::new("POST", "/items?source=test", reply)
    .with_body(br#"{"name":"example"}"#.to_vec());
# Ok(())
# }
```

Responses support final statuses 200–599. The server adds `Content-Length` and
`Connection: close`. Header names and values are validated, including CR/LF
injection; framing headers cannot be overridden. Invalid public `Behavior`
values are also checked at startup. HEAD, 204, and 304 responses suppress bodies.

## Fault injection

```rust,no_run
use service_sim_rs::{Action, Behavior, Rule};
use std::time::Duration;

let scripted = Rule::new("GET", "/upstream",
    Action::new(Behavior::Reset))
    .then(Action::new(Behavior::Timeout))
    .then(Action::new(Behavior::Raw(b"invalid HTTP\r\n".to_vec())))
    .then(Action::respond(200, "recovered")
        .delayed(Duration::from_millis(100)));
```

- `Respond`: framed status and byte body, with optional extra headers.
- `Raw`: verbatim bytes followed by orderly close; use for malformed responses.
- `Reset`: abortive socket close using zero `SO_LINGER`, producing a TCP reset.
  Your HTTP client may wrap the OS error or transparently retry it.
- `Timeout`: keep the socket open and send nothing until server shutdown.
- `delayed`: latency before any behavior, including faults.

Each match atomically advances its rule's action sequence, and the final action
repeats indefinitely. Assignment follows the order in which completed requests
are matched, not TCP arrival order. Concurrent callers receive unique sequence
positions, but their scheduling order is intentionally unspecified. Sequential
requests produce deterministic fault scripts without random state.

## Resource limits and cleanup

```rust,no_run
use service_sim_rs::{Limits, Server};
use std::time::Duration;
# #[tokio::main(flavor = "current_thread")]
# async fn main() -> std::io::Result<()> {
let server = Server::with_limits(vec![], Limits {
    request_bytes: 64 * 1024,
    connections: 16,
    read_timeout: Duration::from_secs(1),
}).await?;
server.shutdown().await?;
# Ok(())
# }
```

Defaults are a 1 MiB total request size, 64 accepted connections, and a 5 second
deadline for a complete request. Requests have at most 64 headers. Oversized,
malformed, duplicate `Content-Length`, unsupported transfer encodings, and
incomplete requests receive 400 when possible. Limits bound buffered requests
and active tasks; excess connections remain in the OS listener backlog. Timeout
faults occupy a connection slot until shutdown, so configure enough capacity for
the desired scenario. Response bodies and rules are supplied by the caller and
are not capped by these request limits.

`shutdown().await` stops acceptance, cancels and joins all connection tasks, and
closes active sockets. Dropping the server also signals cleanup, but requires
the runtime to continue polling. Prefer explicit shutdown before ending a test.
Bind addresses always use ephemeral ports on `127.0.0.1`.

## Scope

HTTP/1.x, one request per connection, and `Content-Length` request bodies only.
No TLS, HTTP/2, chunked requests, keep-alive, pipelining, WebSockets, regex
matching, proxying, or broker simulation. `Expect: 100-continue` is unsupported;
clients must send the complete body without waiting for an interim response.
Use `Raw` for intentionally invalid wire data. This is a test service, not an
internet-facing production HTTP server.

## Development

```sh
cargo fmt --all -- --check
cargo clippy --locked --all-targets --all-features -- -D warnings
cargo test --locked --all-features
cargo build --locked --all-targets --all-features
cargo run --locked --example retry
RUSTDOCFLAGS="-D warnings" cargo doc --locked --no-deps
```

Network integration tests cover matching, sequence progression, concurrent
hits, body fragmentation, headers and injection rejection, latency, timeouts,
true reset errors, malformed responses, bounded input, read deadlines, HEAD,
invalid configuration, shutdown, and drop cleanup. CI checks Rust 1.85 and
stable on Linux and Windows. Only three direct dependencies: Tokio, httparse,
and socket2. Contributions should include a focused test and pass these checks.

MIT licensed; see [LICENSE](LICENSE).
