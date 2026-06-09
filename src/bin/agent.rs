use std::env;
use std::error::Error;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    println!("=== MACHA TUNNEL CLI CLIENT ===");

    // Extract dynamic configurations from shell runtime variables
    let server_ip = env::var("TUNNEL_SERVER_IP").unwrap_or_else(|_| "127.0.0.1".to_string());
    let local_port_str = env::var("LOCAL_DASHBOARD_PORT").unwrap_or_else(|_| "3000".to_string());
    let subdomain = env::var("USER_SUBDOMAIN").unwrap_or_else(|_| "default-user".to_string());

    let local_port: u16 = local_port_str.parse().expect("Error: Invalid port number provided!");

    // Instantiate and fire up the engine straight out of our library module!
    let tunnel = macha::MachaTunnel::new(&server_ip, local_port, &subdomain);
    
    tunnel.start().await?;

    // Keep the main thread alive indefinitely while background loops pump data
    std::future::pending::<()>().await;

    Ok(())
}