use std::io;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

/// A one-shot HTTP/2 proxy that forwards a gRPC request but closes the
/// connection before the first non-empty response DATA frame reaches the
/// client. The server has produced its unary response at that point, allowing
/// an idempotency retry to exercise the real lost-ack boundary.
#[derive(Debug)]
pub struct GrpcLostResponseProxy {
    endpoint: String,
    dropped: Option<oneshot::Receiver<Result<(), String>>>,
    task: JoinHandle<()>,
}

impl GrpcLostResponseProxy {
    pub async fn start(target_endpoint: &str) -> Self {
        let target = target_endpoint
            .strip_prefix("http://")
            .unwrap_or_else(|| {
                panic!("lost-response proxy only supports http endpoints: {target_endpoint}")
            })
            .to_string();
        let listener = TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))
            .await
            .expect("bind lost-response proxy");
        let endpoint = format!(
            "http://127.0.0.1:{}",
            listener
                .local_addr()
                .expect("lost-response proxy local address")
                .port()
        );
        let (dropped_tx, dropped_rx) = oneshot::channel();
        let task = tokio::spawn(async move {
            let result = proxy_one_response(listener, &target)
                .await
                .map_err(|error| error.to_string());
            if let Err(error) = &result {
                eprintln!("[anvil-test] lost-response proxy failed: {error}");
            }
            let _ = dropped_tx.send(result);
        });
        Self {
            endpoint,
            dropped: Some(dropped_rx),
            task,
        }
    }

    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    pub async fn wait_until_response_dropped(&mut self, timeout: Duration) -> Result<(), String> {
        let receiver = self
            .dropped
            .take()
            .expect("lost-response proxy drop was already observed");
        tokio::time::timeout(timeout, receiver)
            .await
            .map_err(|_| "timed out waiting for gRPC response fault".to_string())?
            .map_err(|_| {
                "lost-response proxy task exited without reporting its result".to_string()
            })?
    }
}

impl Drop for GrpcLostResponseProxy {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn proxy_one_response(listener: TcpListener, target: &str) -> io::Result<()> {
    let (client, _) = listener.accept().await?;
    let server = TcpStream::connect(target).await?;
    let (mut client_read, mut client_write) = client.into_split();
    let (mut server_read, mut server_write) = server.into_split();

    let upstream = tokio::spawn(async move {
        let result = tokio::io::copy(&mut client_read, &mut server_write).await;
        let _ = server_write.shutdown().await;
        result
    });

    let downstream = forward_until_response_data(&mut server_read, &mut client_write).await;
    let _ = client_write.shutdown().await;
    upstream.abort();
    downstream
}

async fn forward_until_response_data(
    server: &mut tokio::net::tcp::OwnedReadHalf,
    client: &mut tokio::net::tcp::OwnedWriteHalf,
) -> io::Result<()> {
    loop {
        let mut header = [0_u8; 9];
        match server.read_exact(&mut header).await {
            Ok(_) => {}
            Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "upstream gRPC response ended before a non-empty DATA frame",
                ));
            }
            Err(error) => return Err(error),
        }
        let payload_len =
            (usize::from(header[0]) << 16) | (usize::from(header[1]) << 8) | usize::from(header[2]);
        let frame_type = header[3];
        let stream_id =
            u32::from_be_bytes([header[5], header[6], header[7], header[8]]) & 0x7fff_ffff;
        let mut payload = vec![0_u8; payload_len];
        server.read_exact(&mut payload).await?;

        if frame_type == 0 && stream_id != 0 && payload_len != 0 {
            return Ok(());
        }

        client.write_all(&header).await?;
        client.write_all(&payload).await?;
        client.flush().await?;
    }
}
