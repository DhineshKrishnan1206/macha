use tokio::net::TcpStream;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use std::env;

#[tokio::main]
async fn main() {
    println!("=== PRODUCTION TUNNEL AGENT ===");

    // 1. Read parameters dynamically from environment or fallbacks
    let server_ip = env::var("TUNNEL_SERVER_IP").unwrap_or_else(|_| "127.0.0.1".to_string());
    let local_port = env::var("LOCAL_DASHBOARD_PORT").unwrap_or_else(|_| "3000".to_string());
    let subdomain_id = env::var("USER_SUBDOMAIN").unwrap_or_else(|_| "dhinesh-dashboard".to_string());

    println!("Targeting Server: {}, Local Dashboard Port: {}", server_ip, local_port);

    // 2. Connect to Control Channel
    match TcpStream::connect(format!("{}:9000", server_ip)).await {
        Ok(mut control_stream) => {
            println!("Agent: Connected to control plane.");
            
            // Register our identity string right away so the server maps traffic to us!
            let _ = control_stream.write_all(format!("{}\n", subdomain_id).as_bytes()).await;

            let mut buffer = [0; 1024];
            loop {
                match control_stream.read(&mut buffer).await {
                    Ok(0) => break, // Connection dropped
                    Ok(bytes_read) => {
                        let command = String::from_utf8_lossy(&buffer[..bytes_read]);
                        
                        if command.contains("NEW_CONNECTION") {
                            let server_ip_clone = server_ip.clone();
                            let local_port_clone = local_port.clone();
                            let subdomain_clone = subdomain_id.clone();

                            // Spawn data lane worker safely
                            tokio::spawn(async move {
                                let server_data = TcpStream::connect(format!("{}:9001", server_ip_clone)).await;
                                let local_app = TcpStream::connect(format!("127.0.0.1:{}", local_port_clone)).await;

                                if let (Ok(mut server_stream), Ok(mut local_stream)) = (server_data, local_app) {
                                    // Identify this data channel line to the server loading dock
                                    let _ = server_stream.write_all(format!("{}\n", subdomain_clone).as_bytes()).await;

                                    let (mut server_r, mut server_w) = server_stream.into_split();
                                    let (mut local_r, mut local_w) = local_stream.into_split();

                                    let _ = tokio::join!(
                                        tokio::io::copy(&mut server_r, &mut local_w),
                                        tokio::io::copy(&mut local_r, &mut server_w)
                                    );
                                }
                            });
                        }
                    }
                    Err(_) => break,
                }
            }
        }
        Err(e) => println!("Connection failed: {}", e),
    }
}