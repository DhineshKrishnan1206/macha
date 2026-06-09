use std::error::Error;
use tokio::net::TcpListener;
use tokio::io::AsyncWriteExt;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    println!("📦 Starting 3rd-Party App with Embedded Macha Tunnel...");

    // 1. Simulate a local running application (e.g., a web server on Port 5000)
    tokio::spawn(async {
        let listener = TcpListener::bind("127.0.0.1:5000").await.unwrap();
        println!("🚀 Local App Server is running silently on http://localhost:5000");
        
        loop {
            if let Ok((mut stream, _)) = listener.accept().await {
                tokio::spawn(async move {
                    let response = b"HTTP/1.1 200 OK\r\nContent-Length: 26\r\n\r\nHello from Embedded Macha!";
                    let _ = stream.write_all(response).await;
                });
            }
        }
    });

    // Give our fake app a brief second to spin up
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // 2. IMPORT AND USE THE MACHA LIBRARY DIRECTLY INSIDE CODE
    // We point it to localhost (127.0.0.1), proxying our app's port 5000
    let tunnel = macha::MachaTunnel::new("127.0.0.1", 5000, "macha-test-client");
    
    println!("🔗 Triggering MachaTunnel runtime engine...");
    tunnel.start().await?;

    // Keep our demo app alive to process incoming traffic
    std::future::pending::<()>().await;

    Ok(())
}