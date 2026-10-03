//! Real socket integration tests for scripted HTTP responses and faults.
use service_sim_rs::{Action, Behavior, Limits, Rule, Server};
use std::{io, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

async fn request(server: &Server, bytes: &[u8]) -> io::Result<Vec<u8>> {
    let mut socket = TcpStream::connect(server.address()).await?;
    socket.write_all(bytes).await?;
    let mut response = Vec::new();
    tokio::time::timeout(Duration::from_secs(2), socket.read_to_end(&mut response)).await??;
    Ok(response)
}
const GET: &[u8] = b"GET /test HTTP/1.1\r\nHost: localhost\r\n\r\n";

#[tokio::test]
async fn custom_headers_and_injection_validation() {
    let action = Action::respond(200, "{}")
        .with_header("Content-Type", "application/json")
        .unwrap();
    let server = Server::start(vec![Rule::new("GET", "/test", action)])
        .await
        .unwrap();
    let bytes = request(&server, GET).await.unwrap();
    assert!(String::from_utf8(bytes)
        .unwrap()
        .contains("Content-Type: application/json\r\n"));
    for (name, value) in [
        ("Bad Name", "value"),
        ("X-Test", "bad\r\nInjected: true"),
        ("content-LENGTH", "10"),
        ("Transfer-Encoding", "chunked"),
        ("X-Test", "\0"),
    ] {
        assert!(Action::respond(200, "").with_header(name, value).is_err());
    }
    assert!(Action::new(Behavior::Timeout)
        .with_header("X-Test", "value")
        .is_err());
    assert!(Server::start(vec![Rule::new(
        "GET",
        "/",
        Action::new(Behavior::Respond {
            status: 200,
            body: vec![],
            headers: vec![("X-Test".into(), "bad\r\n".into())]
        })
    )])
    .await
    .is_err());
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn matching_sequence_and_first_rule_priority() {
    let server = Server::start(vec![
        Rule::new("GET", "/test", Action::respond(503, "retry")).then(Action::respond(200, "ok")),
        Rule::new("GET", "/test", Action::respond(500, "wrong")),
    ])
    .await
    .unwrap();
    assert!(request(&server, GET)
        .await
        .unwrap()
        .starts_with(b"HTTP/1.1 503"));
    for _ in 0..2 {
        assert!(request(&server, GET).await.unwrap().ends_with(b"ok"));
    }
    assert_eq!(server.hits(0), Some(3));
    assert_eq!(server.hits(1), Some(0));
    assert_eq!(server.hits(2), None);
    assert!(request(&server, b"GET /missing HTTP/1.1\r\n\r\n")
        .await
        .unwrap()
        .starts_with(b"HTTP/1.1 404"));
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn fragmented_body_and_query_matching() {
    let server = Server::start(vec![Rule::new(
        "POST",
        "/test?q=1",
        Action::respond(201, "created"),
    )
    .with_body(b"abcd".to_vec())])
    .await
    .unwrap();
    let mut socket = TcpStream::connect(server.address()).await.unwrap();
    socket
        .write_all(b"POST /test?q=1 HTTP/1.1\r\nContent-Length: 4\r\n\r\nab")
        .await
        .unwrap();
    tokio::task::yield_now().await;
    socket.write_all(b"cd").await.unwrap();
    let mut response = Vec::new();
    socket.read_to_end(&mut response).await.unwrap();
    assert!(response.ends_with(b"created"));
    assert!(request(
        &server,
        b"POST /test?q=1 HTTP/1.1\r\nContent-Length: 4\r\n\r\nxxxx"
    )
    .await
    .unwrap()
    .starts_with(b"HTTP/1.1 404"));
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn latency_is_observable() {
    let server = Server::start(vec![Rule::new(
        "GET",
        "/test",
        Action::respond(200, "slow").delayed(Duration::from_millis(80)),
    )])
    .await
    .unwrap();
    let started = std::time::Instant::now();
    request(&server, GET).await.unwrap();
    assert!(started.elapsed() >= Duration::from_millis(80));
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn timeout_shutdown_closes_active_socket_and_listener() {
    let server = Server::start(vec![Rule::new(
        "GET",
        "/test",
        Action::new(Behavior::Timeout),
    )])
    .await
    .unwrap();
    let address = server.address();
    let mut socket = TcpStream::connect(address).await.unwrap();
    socket.write_all(GET).await.unwrap();
    let mut byte = [0];
    assert!(
        tokio::time::timeout(Duration::from_millis(50), socket.read(&mut byte))
            .await
            .is_err()
    );
    assert_eq!(server.hits(0), Some(1));
    server.shutdown().await.unwrap();
    assert_eq!(socket.read(&mut byte).await.unwrap(), 0);
    assert!(TcpStream::connect(address).await.is_err());
}

#[tokio::test]
async fn raw_response_is_exact_and_reset_is_transport_error() {
    let server = Server::start(vec![Rule::new(
        "GET",
        "/test",
        Action::new(Behavior::Raw(b"not HTTP!".to_vec())),
    )
    .then(Action::new(Behavior::Reset))])
    .await
    .unwrap();
    assert_eq!(request(&server, GET).await.unwrap(), b"not HTTP!");
    let error = request(&server, GET).await.unwrap_err();
    assert!(matches!(
        error.kind(),
        io::ErrorKind::ConnectionReset | io::ErrorKind::ConnectionAborted
    ));
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn malformed_and_oversized_requests_are_rejected() {
    let server = Server::with_limits(
        vec![],
        Limits {
            request_bytes: 128,
            ..Limits::default()
        },
    )
    .await
    .unwrap();
    for bytes in [
        b"POST / HTTP/1.1\r\nContent-Length: 999\r\n\r\n".as_slice(),
        b"POST / HTTP/1.1\r\nContent-Length: 1\r\nContent-Length: 1\r\n\r\nx",
        b"POST / HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\n",
        b"POST / HTTP/1.1\r\nContent-Length: abc\r\n\r\n",
    ] {
        assert!(request(&server, bytes)
            .await
            .unwrap()
            .starts_with(b"HTTP/1.1 400"));
    }
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn read_deadline_and_drop_cleanup() {
    let server = Server::with_limits(
        vec![],
        Limits {
            read_timeout: Duration::from_millis(30),
            ..Limits::default()
        },
    )
    .await
    .unwrap();
    let address = server.address();
    let mut socket = TcpStream::connect(address).await.unwrap();
    socket.write_all(b"GET /").await.unwrap();
    let mut response = Vec::new();
    tokio::time::timeout(Duration::from_secs(1), socket.read_to_end(&mut response))
        .await
        .unwrap()
        .unwrap();
    assert!(response.starts_with(b"HTTP/1.1 400"));
    drop(server);
    tokio::time::timeout(Duration::from_secs(1), async {
        while TcpStream::connect(address).await.is_ok() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn concurrent_requests_do_not_lose_hits() {
    let server = Server::start(vec![Rule::new("GET", "/test", Action::respond(200, "ok"))])
        .await
        .unwrap();
    let mut tasks = tokio::task::JoinSet::new();
    for _ in 0..20 {
        let address = server.address();
        tasks.spawn(async move {
            let mut socket = TcpStream::connect(address).await.unwrap();
            socket.write_all(GET).await.unwrap();
            let mut bytes = Vec::new();
            socket.read_to_end(&mut bytes).await.unwrap();
            assert!(bytes.ends_with(b"ok"));
        });
    }
    while let Some(result) = tasks.join_next().await {
        result.unwrap();
    }
    assert_eq!(server.hits(0), Some(20));
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn invalid_configuration_is_rejected_and_head_has_no_body() {
    assert!(Server::with_limits(
        vec![],
        Limits {
            connections: 0,
            ..Limits::default()
        }
    )
    .await
    .is_err());
    assert!(
        Server::start(vec![Rule::new("GET", "/", Action::respond(999, ""))])
            .await
            .is_err()
    );
    let server = Server::start(vec![Rule::new(
        "HEAD",
        "/",
        Action::respond(200, "invisible"),
    )])
    .await
    .unwrap();
    assert!(request(&server, b"HEAD / HTTP/1.1\r\n\r\n")
        .await
        .unwrap()
        .ends_with(b"\r\n\r\n"));
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn connection_limit_bounds_stalled_fault_tasks() {
    let server = Server::with_limits(
        vec![Rule::new("GET", "/test", Action::new(Behavior::Timeout))],
        Limits {
            connections: 1,
            ..Limits::default()
        },
    )
    .await
    .unwrap();
    let mut first = TcpStream::connect(server.address()).await.unwrap();
    first.write_all(GET).await.unwrap();
    tokio::time::timeout(Duration::from_secs(1), async {
        while server.hits(0) != Some(1) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let mut second = TcpStream::connect(server.address()).await.unwrap();
    second.write_all(GET).await.unwrap();
    let mut byte = [0];
    assert!(
        tokio::time::timeout(Duration::from_millis(40), second.read(&mut byte))
            .await
            .is_err()
    );
    assert_eq!(server.hits(0), Some(1));
    server.shutdown().await.unwrap();
    assert_eq!(first.read(&mut byte).await.unwrap(), 0);
    // The unaccepted backlog socket can close orderly or receive a reset.
    match tokio::time::timeout(Duration::from_secs(1), second.read(&mut byte))
        .await
        .unwrap()
    {
        Ok(0) => {}
        Err(error) => assert!(matches!(
            error.kind(),
            io::ErrorKind::ConnectionReset | io::ErrorKind::ConnectionAborted
        )),
        other => panic!("unexpected socket state: {other:?}"),
    }
}
