//! Demonstrate a simulated transient failure followed by successful recovery.
use service_sim_rs::{Action, Rule, Server};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

#[tokio::main(flavor = "current_thread")]
async fn main() -> std::io::Result<()> {
    let server = Server::start(vec![Rule::new(
        "GET",
        "/health",
        Action::respond(503, "retry"),
    )
    .then(Action::respond(200, "ready"))])
    .await?;
    for expected in [503, 200] {
        let mut socket = TcpStream::connect(server.address()).await?;
        socket
            .write_all(b"GET /health HTTP/1.1\r\nHost: localhost\r\n\r\n")
            .await?;
        let mut response = String::new();
        socket.read_to_string(&mut response).await?;
        assert!(response.starts_with(&format!("HTTP/1.1 {expected}")));
        println!("{}", response.lines().next().unwrap());
    }
    server.shutdown().await
}
