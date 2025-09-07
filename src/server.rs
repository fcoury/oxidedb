use crate::config::Config;
use crate::error::{Error, Result};
use crate::protocol::{MessageHeader, OP_MSG};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

pub async fn run(cfg: Config) -> Result<()> {
    let listener = TcpListener::bind(&cfg.listen_addr).await?;
    tracing::info!(listen_addr = %cfg.listen_addr, "oxidedb listening");

    loop {
        let (socket, addr) = listener.accept().await?;
        tracing::debug!(%addr, "accepted connection");
        tokio::spawn(async move {
            if let Err(e) = handle_connection(socket).await {
                tracing::debug!(error = %format!("{e:?}"), "connection closed with error");
            }
        });
    }
}

async fn handle_connection(mut socket: TcpStream) -> Result<()> {
    // Read a single message header to observe op code (best-effort)
    let mut header_buf = [0u8; 16];
    let n = socket.read(&mut header_buf).await?;
    if n == 0 {
        return Ok(());
    }
    if let Some((hdr, _)) = MessageHeader::parse(&header_buf[..n]) {
        tracing::trace!(?hdr, "received header");
        match hdr.op_code {
            OP_MSG => {
                // For now, we don't parse the body; just drain it and noop.
                // In the next iteration, we will parse the BSON command document and respond to hello/ping.
                drain(&mut socket, hdr.message_length as usize - 16).await?;
            }
            _ => {
                tracing::warn!(op_code = hdr.op_code, "unsupported op code");
                // Attempt to drain whatever remains to keep the connection stable
                if hdr.message_length > 16 {
                    let _ = drain(&mut socket, hdr.message_length as usize - 16).await;
                }
            }
        }
    } else {
        tracing::warn!("incomplete or invalid header");
    }

    // Politely close for now
    let _ = socket.shutdown().await;
    Ok(())
}

async fn drain(stream: &mut TcpStream, mut remaining: usize) -> Result<()> {
    let mut buf = [0u8; 4096];
    while remaining > 0 {
        let to_read = remaining.min(buf.len());
        let n = stream.read(&mut buf[..to_read]).await?;
        if n == 0 { break; }
        remaining -= n;
    }
    Ok(())
}

