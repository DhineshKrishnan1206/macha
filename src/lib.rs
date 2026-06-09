use tokio::net::TcpStream;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use std::error::Error;
use std::time::Duration;

pub struct MachaTunnel {
    server_ip: String,
    control_port: u16,
    data_port: u16,
    local_port: u16,
    subdomain: String,
}

impl MachaTunnel {
    /// Creates a new configuration instance for a Macha tunnel bridge.
    pub fn new(server_ip: &str, local_port: u16, subdomain: &str) -> Self {
        Self {
            server_ip: server_ip.to_string(),
            control_port: 9000,
            data_port: 9001,
            local_port,
            subdomain: subdomain.to_string(),
        }
    }

    /// Overrides the default control plane port (9000) if required.
    pub fn with_control_port(mut self, port: u16) -> Self {
        self.control_port = port;
        self
    }

    /// Overrides the default data tunnel port (9001) if required.
    pub fn with_data_port(mut self, port: u16) -> Self {
        self.data_port = port;
        self
    }

    /// Spawns the background runtime workers to establish and preserve the internet tunnel.
    pub async fn start(&self) -> Result<(), Box<dyn Error>> {
        println!("Macha Lib: Target cloud infrastructure -> {}:{}", self.server_ip, self.control_port);
        
        // 1. Establish the foundational Control Plane Channel
        let mut control_stream = TcpStream::connect(format!("{}:{}", self.server_ip, self.control_port)).await?;
        
        // 2. Transmit the identification handshake registration packet
        control_stream.write_all(self.subdomain.as_bytes()).await?;
        println!("Macha Lib: Successfully registered active subdomain: '{}'", self.subdomain);

        let local_port = self.local_port;
        let server_ip = self.server_ip.clone();
        let data_port = self.data_port;
        let subdomain = self.subdomain.clone();

        // 3. Keep the control consumer loop active in a non-blocking background frame
        tokio::spawn(async move {
            let mut buf = [0; 1024];
            loop {
                match control_stream.read(&mut buf).await {
                    Ok(0) => {
                        println!("Macha Lib: Control plane severed by remote host.");
                        break;
                    }
                    Ok(n) => {
                        let signal = String::from_utf8_lossy(&buf[..n]);
                        if signal.contains("NEW_CONNECTION") {
                            println!("Macha Lib: Traffic signal received. Spawning data lane...");
                            
                            let server_ip_clone = server_ip.clone();
                            let subdomain_clone = subdomain.clone();

                            // Spin up an isolated parallel data delivery worker thread
                            tokio::spawn(async move {
                                if let Err(e) = Self::bridge_traffic(&server_ip_clone, data_port, local_port, &subdomain_clone).await {
                                    println!("Macha Lib Data Error: Forwarding execution failure: {}", e);
                                }
                            });
                        }
                    }
                    Err(e) => {
                        println!("Macha Lib Error: Failed reading control channel frames: {}", e);
                        break;
                    }
                }
            }
        });

        Ok(())
    }

    /// Connects your local server port directly to the cloud's dedicated data port.
    async fn bridge_traffic(server_ip: &str, data_port: u16, local_port: u16, subdomain: &str) -> Result<(), Box<dyn Error>> {
        // Double handshake connection setup
        let mut cloud_data_stream = TcpStream::connect(format!("{}:{}", server_ip, data_port)).await?;
        let mut local_app_stream = TcpStream::connect(format!("127.0.0.1:{}", local_port)).await?;

        // Authenticate this explicit data stream line with our subdomain tracking ID
        cloud_data_stream.write_all(subdomain.as_bytes()).await?;
        
        // Wait a tiny bit for the cloud coordinator to successfully split and lock channels
        tokio::time::sleep(Duration::from_millis(15)).await;

        // Perform raw byte splitting and asynchronous continuous mirroring
        let (mut cloud_reader, mut cloud_writer) = cloud_data_stream.into_split();
        let (mut local_reader, mut local_writer) = local_app_stream.into_split();

        let inbound_lane = tokio::io::copy(&mut cloud_reader, &mut local_writer);
        let outbound_lane = tokio::io::copy(&mut local_reader, &mut cloud_writer);

        tokio::join!(inbound_lane, outbound_lane);
        Ok(())
    }
}