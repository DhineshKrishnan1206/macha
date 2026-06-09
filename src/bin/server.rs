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
            public_conn = public_listener.accept() => {
                if let Ok((client_stream, _)) = public_conn {
                    let target_subdomain = "dhinesh-dashboard".to_string(); 

                    let agent_tx = {
                        let lock = agents.lock().unwrap();
                        lock.get(&target_subdomain).cloned()
                    };

                    if let Some(control_tx) = agent_tx {
                        println!("Server: Visitor arrived. Creating slot and signaling agent...");
                        
                        let (visitor_tx, mut visitor_rx) = mpsc::channel::<TcpStream>(1);
                        {
                            let mut lock = pending_visitors.lock().unwrap();
                            lock.insert(target_subdomain.clone(), visitor_tx);
                        }

                        // Send signal to agent over control channel
                        let _ = control_tx.send(client_stream).await;
                    } else {
                        println!("Server Warning: Visitor arrived but agent is offline!");
                    }
                }
            }

            // DOOR 2: Agent registers Control Channel
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
                                    // We wrap the stream in an Option so we can safely extract it ONCE
                                    let mut stream_container = Some(visitor_stream);
                                    let mut attempts = 0;
                                    
                                    while attempts < 50 {
                                        let matched_sender = {
                                            let lock = pending_visitors_clone.lock().unwrap();
                                            lock.get(&subdomain_clone).cloned()
                                        };
                                        
                                        if let Some(tx) = matched_sender {
                                            // Take the stream out of the container leaving None behind
                                            if let Some(stream) = stream_container.take() {
                                                if let Err(mpsc::error::SendError(returned_stream)) = tx.send(stream).await {
                                                    // If sending failed, put the stream BACK into our container for the next attempt!
                                                    stream_container = Some(returned_stream);
                                                } else {
                                                    // Success! The stream was safely passed to port 9001
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
                            
                            // Create a channel slot to receive our visitor stream
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