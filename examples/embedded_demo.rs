/// Demonstrates embedding a Macha tunnel inside a Rust application.
/// Run with: cargo run --example embedded_demo
use macha::Tunnel;
use tokio::{io::AsyncWriteExt, net::TcpListener};

#[tokio::main]
async fn main() -> macha::Result<()> {
    // 1. Simulate a local web server on port 5000.
    tokio::spawn(async {
        let listener = TcpListener::bind("127.0.0.1:5000").await.unwrap();
        println!("Local app listening on http://localhost:5000");
        loop {
            if let Ok((mut stream, _)) = listener.accept().await {
                tokio::spawn(async move {
                    let body = b"Hello from the embedded Macha demo!";
                    let response = format!(
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                        body.len()
                    );
                    let _ = stream.write_all(response.as_bytes()).await;
                    let _ = stream.write_all(body).await;
                });
            }
        }
    });

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // 2. Expose it via macha using the builder API.
    Tunnel::builder("demo", 5000)
        .server("127.0.0.1") // point at local server for testing
        .reconnect(false)    // fail fast in the demo
        .build()?
        .run()
        .await
}
