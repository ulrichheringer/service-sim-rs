//! Deterministic HTTP/1 service simulation over real loopback sockets.
//!
//! Rules match in declaration order. A rule's action sequence advances on each
//! matching request, then repeats its final action. See [`Server::start`].

use std::{
    io,
    net::SocketAddr,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{oneshot, Semaphore},
    task::{JoinHandle, JoinSet},
};

/// A response or transport fault.
#[derive(Clone, Debug)]
pub enum Behavior {
    /// Return a status and byte body with automatic framing.
    Respond {
        /// Final HTTP status (200 through 599).
        status: u16,
        /// Body bytes.
        body: Vec<u8>,
        /// Additional headers; framing headers are reserved and validated at startup.
        headers: Vec<(String, String)>,
    },
    /// Write these bytes verbatim, allowing malformed HTTP responses.
    Raw(Vec<u8>),
    /// Send no response; retain the socket until server shutdown.
    Timeout,
    /// Abort the socket using zero linger (TCP reset).
    Reset,
}

/// An action with optional latency before its behavior.
#[derive(Clone, Debug)]
pub struct Action {
    /// Latency before execution.
    pub latency: Duration,
    /// Response or fault to execute.
    pub behavior: Behavior,
}
impl Action {
    /// Construct an action without latency.
    pub fn new(behavior: Behavior) -> Self {
        Self {
            latency: Duration::ZERO,
            behavior,
        }
    }
    /// Construct a framed response.
    pub fn respond(status: u16, body: impl Into<Vec<u8>>) -> Self {
        Self::new(Behavior::Respond {
            status,
            body: body.into(),
            headers: Vec::new(),
        })
    }
    /// Add a response header. Rejects invalid bytes and reserved framing headers.
    /// Returns `InvalidInput` if this action is a fault rather than a response.
    pub fn with_header(
        mut self,
        name: impl Into<String>,
        value: impl Into<String>,
    ) -> io::Result<Self> {
        let name = name.into();
        let value = value.into();
        if !valid_header(&name, &value) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid or reserved response header",
            ));
        }
        match &mut self.behavior {
            Behavior::Respond { headers, .. } => headers.push((name, value)),
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "only responses have headers",
                ))
            }
        }
        Ok(self)
    }
    /// Set latency.
    pub fn delayed(mut self, latency: Duration) -> Self {
        self.latency = latency;
        self
    }
}

/// Exact method and request-target matcher, with an optional exact body match.
#[derive(Clone, Debug)]
pub struct Rule {
    method: String,
    target: String,
    body: Option<Vec<u8>>,
    actions: Vec<Action>,
}
impl Rule {
    /// Match an exact HTTP method and request target (including query string).
    pub fn new(method: impl Into<String>, target: impl Into<String>, action: Action) -> Self {
        Self {
            method: method.into(),
            target: target.into(),
            body: None,
            actions: vec![action],
        }
    }
    /// Restrict matching to an exact byte body.
    pub fn with_body(mut self, body: impl Into<Vec<u8>>) -> Self {
        self.body = Some(body.into());
        self
    }
    /// Append an action to the deterministic sequence; the last action repeats.
    pub fn then(mut self, action: Action) -> Self {
        self.actions.push(action);
        self
    }
}

/// Resource limits for untrusted or stalled clients.
#[derive(Clone, Debug)]
pub struct Limits {
    /// Maximum total request bytes, including headers (default 1 MiB).
    pub request_bytes: usize,
    /// Maximum simultaneous accepted connections (default 64).
    pub connections: usize,
    /// Deadline for receiving a complete request (default 5 seconds).
    pub read_timeout: Duration,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            request_bytes: 1024 * 1024,
            connections: 64,
            read_timeout: Duration::from_secs(5),
        }
    }
}

/// A running, ephemeral-port IPv4 loopback server.
///
/// [`Self::shutdown`] joins all tasks. Dropping the handle signals shutdown;
/// cleanup runs on the Tokio runtime on its next scheduling opportunity.
pub struct Server {
    address: SocketAddr,
    counts: Arc<Vec<AtomicUsize>>,
    stop: Option<oneshot::Sender<()>>,
    task: Option<JoinHandle<io::Result<()>>>,
}
impl Server {
    /// Start using default resource limits. Requires a running Tokio runtime.
    pub async fn start(rules: Vec<Rule>) -> io::Result<Self> {
        Self::with_limits(rules, Limits::default()).await
    }
    /// Start with explicit limits. Invalid limits and status codes are rejected.
    pub async fn with_limits(rules: Vec<Rule>, limits: Limits) -> io::Result<Self> {
        if limits.request_bytes < 16
            || limits.connections == 0
            || limits.read_timeout.is_zero()
            || rules
                .iter()
                .flat_map(|r| &r.actions)
                .any(|a| match &a.behavior {
                    Behavior::Respond {
                        status, headers, ..
                    } => {
                        !(200..=599).contains(status)
                            || headers
                                .iter()
                                .any(|(name, value)| !valid_header(name, value))
                    }
                    _ => false,
                })
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid limits or HTTP status",
            ));
        }
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let counts = Arc::new(
            (0..rules.len())
                .map(|_| AtomicUsize::new(0))
                .collect::<Vec<_>>(),
        );
        let shared = counts.clone();
        let (stop, mut stopped) = oneshot::channel();
        let task = tokio::spawn(async move {
            let rules = Arc::new(rules);
            let slots = Arc::new(Semaphore::new(limits.connections));
            let mut tasks = JoinSet::new();
            let result = loop {
                tokio::select! {
                    _ = &mut stopped => break Ok(()),
                    Some(_) = tasks.join_next(), if !tasks.is_empty() => {},
                    accepted = async {
                        let permit = slots.clone().acquire_owned().await.map_err(io::Error::other)?;
                        let (stream, _) = listener.accept().await?;
                        Ok::<_, io::Error>((stream, permit))
                    } => match accepted {
                        Ok((stream, permit)) => {
                            let rules = rules.clone(); let counts = shared.clone(); let limits = limits.clone();
                            tasks.spawn(async move { let _permit = permit; let _ = handle(stream, rules, counts, limits).await; });
                        },
                        Err(error) => break Err(error),
                    }
                }
            };
            tasks.abort_all();
            while tasks.join_next().await.is_some() {}
            result
        });
        Ok(Self {
            address,
            counts,
            stop: Some(stop),
            task: Some(task),
        })
    }
    /// Bound loopback address.
    pub fn address(&self) -> SocketAddr {
        self.address
    }
    /// Base URL for HTTP clients.
    pub fn url(&self) -> String {
        format!("http://{}", self.address)
    }
    /// Count matches for a zero-based rule index, or `None` for an invalid index.
    pub fn hits(&self, rule: usize) -> Option<usize> {
        self.counts.get(rule).map(|c| c.load(Ordering::SeqCst))
    }
    /// Stop accepting and close all active sockets, including timeout faults.
    pub async fn shutdown(mut self) -> io::Result<()> {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        self.task
            .take()
            .expect("server task exists")
            .await
            .map_err(io::Error::other)?
    }
}
impl Drop for Server {
    fn drop(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
    }
}

struct Request {
    method: String,
    target: String,
    body: Vec<u8>,
}
async fn read_request(stream: &mut TcpStream, max: usize) -> io::Result<Request> {
    let mut bytes = Vec::new();
    loop {
        let mut headers = [httparse::EMPTY_HEADER; 64];
        let mut parsed = httparse::Request::new(&mut headers);
        match parsed
            .parse(&bytes)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?
        {
            httparse::Status::Complete(head) => {
                let mut length = None;
                for header in parsed.headers.iter() {
                    if header.name.eq_ignore_ascii_case("transfer-encoding") {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "transfer encoding unsupported",
                        ));
                    }
                    if header.name.eq_ignore_ascii_case("content-length") {
                        if length.is_some() {
                            return Err(io::Error::new(
                                io::ErrorKind::InvalidData,
                                "duplicate content length",
                            ));
                        }
                        length = Some(
                            std::str::from_utf8(header.value)
                                .ok()
                                .and_then(|s| s.trim().parse::<usize>().ok())
                                .ok_or_else(|| {
                                    io::Error::new(
                                        io::ErrorKind::InvalidData,
                                        "invalid content length",
                                    )
                                })?,
                        );
                    }
                }
                let total = head
                    .checked_add(length.unwrap_or(0))
                    .filter(|n| *n <= max)
                    .ok_or_else(|| {
                        io::Error::new(io::ErrorKind::InvalidData, "request too large")
                    })?;
                if bytes.len() >= total {
                    return Ok(Request {
                        method: parsed.method.unwrap().into(),
                        target: parsed.path.unwrap().into(),
                        body: bytes[head..total].to_vec(),
                    });
                }
            }
            httparse::Status::Partial => {}
        }
        if bytes.len() >= max {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "request too large",
            ));
        }
        let mut buffer = [0; 4096];
        let take = buffer.len().min(max - bytes.len());
        let read = stream.read(&mut buffer[..take]).await?;
        if read == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "incomplete request",
            ));
        }
        bytes.extend_from_slice(&buffer[..read]);
    }
}
async fn handle(
    mut stream: TcpStream,
    rules: Arc<Vec<Rule>>,
    counts: Arc<Vec<AtomicUsize>>,
    limits: Limits,
) -> io::Result<()> {
    let request = match tokio::time::timeout(
        limits.read_timeout,
        read_request(&mut stream, limits.request_bytes),
    )
    .await
    {
        Ok(Ok(request)) => request,
        _ => {
            stream
                .write_all(
                    b"HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                )
                .await?;
            return Ok(());
        }
    };
    let action = rules
        .iter()
        .enumerate()
        .find(|(_, r)| {
            r.method == request.method
                && r.target == request.target
                && r.body.as_ref().is_none_or(|b| *b == request.body)
        })
        .map(|(i, r)| {
            let index = counts[i].fetch_add(1, Ordering::SeqCst);
            r.actions[index.min(r.actions.len() - 1)].clone()
        })
        .unwrap_or_else(|| Action::respond(404, "no matching rule"));
    tokio::time::sleep(action.latency).await;
    match action.behavior {
        Behavior::Respond {
            status,
            body,
            headers,
        } => {
            let body = if request.method == "HEAD" || status < 200 || status == 204 || status == 304
            {
                Vec::new()
            } else {
                body
            };
            let mut header = format!(
                "HTTP/1.1 {status} Simulated\r\nContent-Length: {}\r\nConnection: close\r\n",
                body.len()
            );
            for (name, value) in headers {
                header.push_str(&format!("{name}: {value}\r\n"));
            }
            header.push_str("\r\n");
            stream.write_all(header.as_bytes()).await?;
            stream.write_all(&body).await?;
        }
        Behavior::Raw(bytes) => stream.write_all(&bytes).await?,
        Behavior::Timeout => std::future::pending::<()>().await,
        Behavior::Reset => {
            let socket = socket2::Socket::from(stream.into_std()?);
            socket.set_linger(Some(Duration::ZERO))?;
            return Ok(());
        }
    }
    stream.shutdown().await
}

fn valid_header(name: &str, value: &str) -> bool {
    !name.is_empty()
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"!#$%&'*+-.^_`|~".contains(&b))
        && ![
            "content-length",
            "transfer-encoding",
            "connection",
            "trailer",
            "upgrade",
        ]
        .iter()
        .any(|reserved| name.eq_ignore_ascii_case(reserved))
        && value.bytes().all(|b| b == b'\t' || b >= 32 && b != 127)
}
