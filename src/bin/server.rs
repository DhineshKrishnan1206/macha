use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::env;

type SharedAgents = Arc<Mutex<HashMap<String, mpsc::Sender<TcpStream>>>>;

#[tokio::main]
async fn main() {
    println!("=== PRODUCTION TUNNEL SERVER ===");

    let public_port = env::var("PUBLIC_PORT").unwrap_or_else(|_| "8000".to_string());
    let control_port = env::var("CONTROL_PORT").unwrap_or_else(|_| "9000".to_string());
    let data_port = env::var("DATA_PORT").unwrap_or_else(|_| "9001".to_string());

    let public_listener = TcpListener::bind(format!("0.0.0.0:{}", public_port)).await.unwrap();
    let control_listener = TcpListener::bind(format!("0.0.0.0:{}", control_port)).await.unwrap();
    let data_listener = TcpListener::bind(format!("0.0.0.0:{}", data_port)).await.unwrap();

    println!("Listening: Public -> :{}, Control -> :{}, Data -> :{}", public_port, control_port, data_port);

    let agents: SharedAgents = Arc::new(Mutex::new(HashMap::new()));
    
    // A shared lobby where public visitor streams wait, mapped by user subdomain
    let pending_visitors: Arc<Mutex<HashMap<String, mpsc::Sender<TcpStream>>>> = Arc::new(Mutex::new(HashMap::new()));

    loop {
        let agents = agents.clone();
        let pending_visitors = pending_visitors.clone();

        tokio::select! {
            // DOOR 1: Public web visitor hits port 8000
            // DOOR 1: Public web visitor hits port 8000
            public_conn = public_listener.accept() => {
                if let Ok((mut client_stream, _)) = public_conn {
                    let agents_clone = agents.clone();
                    let pending_visitors_clone = pending_visitors.clone();

                    tokio::spawn(async move {
                        // 1. Peek at incoming bytes without consuming them from the TCP buffer yet
                        let mut buf = [0; 1024];
                        if let Ok(n) = client_stream.peek(&mut buf).await {
                            let http_request = String::from_utf8_lossy(&buf[..n]);
                            
                            // 2. Parse out the "Host:" header cleanly
                            let mut target_subdomain = None;
                            for line in http_request.lines() {
                                if line.to_lowercase().starts_with("host:") {
                                    // Strip "host:" prefix and spaces (e.g., "Host: dhinesh.macha.live:8000")
                                    let host_content = line[5..].trim(); 
                                    
                                    // Strip out port if it exists -> "dhinesh.macha.live"
                                    let domain_part = host_content.split(':').next().unwrap_or(host_content);
                                    
                                    // Advanced check: Ensure they are actually using your domain!
                                    if domain_part.ends_with("macha.live") {
                                        // Split by dot and grab the first segment -> "dhinesh"
                                        if let Some(subdomain) = domain_part.split('.').next() {
                                            // Handle the edge case if someone goes to just "macha.live" directly
                                            if subdomain != "macha" && subdomain != "www" {
                                                target_subdomain = Some(subdomain.to_string());
                                            }
                                        }
                                    }
                                    break; 
                                }
                            }

                            // Fallback to a default pool if no valid subdomain was explicitly provided
                            let target_subdomain = target_subdomain.unwrap_or_else(|| "default".to_string());
                            println!("Server: Inbound request targeting subdomain routing register: '{}'", target_subdomain);

                            // 3. Look up the extracted subdomain in our map registry
                            let agent_tx = {
                                let lock = agents_clone.lock().unwrap();
                                lock.get(&target_subdomain).cloned()
                            };

                            if let Some(control_tx) = agent_tx {
                                println!("Server: Routing packet to active agent cluster: {}", target_subdomain);
                                
                                let (visitor_tx, mut visitor_rx) = mpsc::channel::<TcpStream>(1);
                                {
                                    let mut lock = pending_visitors_clone.lock().unwrap();
                                    lock.insert(target_subdomain.clone(), visitor_tx);
                                }

                                // Signal the agent over the control plane
                                let _ = control_tx.send(client_stream).await;
                            } else {
                                println!("Server Warning: Visitor arrived but agent '{}' is offline!", target_subdomain);
                                let response = b"HTTP/1.1 404 Not Found\r\nContent-Length: 26\r\n\r\nMacha Error: Agent Offline";
                                let _ = client_stream.write_all(response).await;
                            }
                        }
                    });
                }
            }
            // DOOR 2: Agent registers Control Channel
            control_conn = control_listener.accept() => {
                if let Ok((mut control_stream, _)) = control_conn {
                    let agents_clone = agents.clone();
                    tokio::spawn(async move {
                        let mut buf = [0; 256];
                        if let Ok(n) = control_stream.read(&mut buf).await {
                            let subdomain = String::from_utf8_lossy(&buf[..n]).trim().to_string();
                            
                            let (tx, mut rx) = mpsc::channel::<TcpStream>(10);
                            agents_clone.lock().unwrap().insert(subdomain.clone(), tx);
                            println!("Server: Registered control plane for user: {}", subdomain);

                            while let Some(visitor_stream) = rx.recv().await {
                                // Signal agent to wake up
                                if let Err(_) = control_stream.write_all(b"NEW_CONNECTION\n").await {
                                    break; 
                                }

                                let pending_visitors_clone = pending_visitors.clone();
                                let subdomain_clone = subdomain.clone();
                                
                                tokio::spawn(async move {
                                    let mut stream_container = Some(visitor_stream);
                                    let mut attempts = 0;
                                    
                                    while attempts < 50 {
                                        let matched_sender = {
                                            let lock = pending_visitors_clone.lock().unwrap();
                                            lock.get(&subdomain_clone).cloned()
                                        };
                                        
                                        if let Some(tx) = matched_sender {
                                            if let Some(stream) = stream_container.take() {
                                                if let Err(mpsc::error::SendError(returned_stream)) = tx.send(stream).await {
                                                    stream_container = Some(returned_stream);
                                                } else {
                                                    break;
                                                }
                                            }
                                        }
                                        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
                                        attempts += 1;
                                    }
                                });
                            }
                            agents_clone.lock().unwrap().remove(&subdomain);
                            println!("Server: User {} disconnected.", subdomain);
                        }
                    });
                }
            }

            // DOOR 3: Agent opens Data Line
            data_conn = data_listener.accept() => {
                if let Ok((mut agent_data_stream, _)) = data_conn {
                    let pending_visitors_clone = pending_visitors.clone();
                    tokio::spawn(async move {
                        let mut buf = [0; 256];
                        if let Ok(n) = agent_data_stream.read(&mut buf).await {
                            let subdomain = String::from_utf8_lossy(&buf[..n]).trim().to_string();
                            
                            let (tx, mut rx) = mpsc::channel::<TcpStream>(1);
                            {
                                let mut lock = pending_visitors_clone.lock().unwrap();
                                lock.insert(subdomain.clone(), tx);
                            }

                            if let Some(mut visitor_stream) = rx.recv().await {
                                println!("Server: Gluing public visitor (8000) and agent data line (9001) together!");
                                
                                let (mut visitor_reader, mut visitor_writer) = visitor_stream.into_split();
                                let (mut agent_reader, mut agent_writer) = agent_data_stream.into_split();

                                let lane_a = tokio::io::copy(&mut visitor_reader, &mut agent_writer);
                                let lane_b = tokio::io::copy(&mut agent_reader, &mut visitor_writer);

                                let _ = tokio::join!(lane_a, lane_b);
                                println!("Server: Tunnel transmission complete.");
                            }
                            
                            let mut lock = pending_visitors_clone.lock().unwrap();
                            lock.remove(&subdomain);
                        }
                    });
                }
            }
        }
    }
}