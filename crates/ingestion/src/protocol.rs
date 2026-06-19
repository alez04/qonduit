//! Wire protocol operations for the Qubic TCP interface.
//!
//! Provides helpers for sending requests, reading responses, and performing
//! the initial peer exchange handshake with a Qubic node.

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use anyhow::{Context, Result};
use tracing::{debug, info, warn};

use qonduit_core::RequestResponseHeader;

/// Send a raw packet (header + optional payload).
pub async fn send_raw(
    stream: &mut TcpStream,
    msg_type: u8,
    payload: &[u8],
    dejavu: u32,
) -> Result<()> {
    let header = RequestResponseHeader::new_request(msg_type, payload.len() as u32, dejavu);
    let header_bytes: [u8; 8] = unsafe { std::mem::transmute(header) };
    stream.write_all(&header_bytes).await?;
    if !payload.is_empty() {
        stream.write_all(payload).await?;
    }
    stream.flush().await?;
    Ok(())
}

/// Perform the initial peer exchange handshake.
///
/// This is fire-and-forget per the Qubic protocol: we send our local peers
/// (type 0, 16 bytes = 4 x 4-byte IPv4 addresses) but the node does NOT
/// respond with its peers. Instead it starts broadcasting data immediately.
pub async fn exchange_public_peers(
    stream: &mut TcpStream,
    local_peers: &[[u8; 4]; 4],
) -> Result<()> {
    let mut payload = Vec::with_capacity(16);
    for peer in local_peers {
        payload.extend_from_slice(peer);
    }
    send_raw(stream, 0, &payload, rand::random()).await?;
    info!("Sent peer exchange (fire-and-forget)");
    Ok(())
}

/// Read a single packet (header + payload) from the stream.
///
/// Returns the message type, dejavu, and payload bytes.
pub async fn read_packet(stream: &mut TcpStream) -> Result<(u8, u32, Vec<u8>)> {
    let mut header_buf = [0u8; 8];
    stream.read_exact(&mut header_buf).await?;

    let header: &RequestResponseHeader =
        unsafe { &*(&header_buf as *const [u8; 8] as *const RequestResponseHeader) };

    let msg_type = header.msg_type();
    let dejavu = header.dejavu();
    let payload_size = header.payload_size() as usize;

    let payload = if payload_size > 0 {
        let mut buf = vec![0u8; payload_size];
        stream.read_exact(&mut buf).await?;
        buf
    } else {
        Vec::new()
    };

    Ok((msg_type, dejavu, payload))
}

/// Request current tick info (type 27, no payload).
///
/// Sends the request and loops reading packets until we get a type 28
/// (RESPOND_CURRENT_TICK_INFO) response. Other packet types received
/// in between are logged and skipped.
///
/// Returns the raw response bytes for the CurrentTickInfo payload.
pub async fn request_current_tick_info(stream: &mut TcpStream) -> Result<Vec<u8>> {
    let dejavu = rand::random::<u32>().max(1); // avoid 0
    send_raw(stream, 27, &[], dejavu).await?;

    // Read packets until we get type 28 (RESPOND_CURRENT_TICK_INFO)
    let deadline = tokio::time::Instant::now() + tokio::time::Duration::from_secs(10);
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            anyhow::bail!("Timeout waiting for CurrentTickInfo response");
        }

        match tokio::time::timeout(remaining, read_packet(stream)).await {
            Ok(Ok((msg_type, _dejavu, payload))) => {
                if msg_type == 28 {
                    // RESPOND_CURRENT_TICK_INFO
                    debug!("Received CurrentTickInfo ({} bytes)", payload.len());
                    return Ok(payload);
                }
                if msg_type == 35 {
                    // END_RESPONSE — shouldn't happen for this but handle it
                    anyhow::bail!("Node sent EndResponse for CurrentTickInfo request");
                }
                if msg_type == 54 {
                    anyhow::bail!("Node returned TryAgain");
                }
                // Other broadcast packet — skip and keep reading
                debug!("Skipping packet type={msg_type} while waiting for CurrentTickInfo");
            }
            Ok(Err(e)) => return Err(e).context("Read error waiting for CurrentTickInfo"),
            Err(_) => anyhow::bail!("Timeout waiting for CurrentTickInfo response"),
        }
    }
}

/// Request computors list (type 11, no payload).
///
/// Loops until we get type 2 (RESPOND_COMPUTOR_LIST) or timeout.
pub async fn request_computors(stream: &mut TcpStream) -> Result<Vec<u8>> {
    let dejavu = rand::random::<u32>().max(1);
    send_raw(stream, 11, &[], dejavu).await?;

    let deadline = tokio::time::Instant::now() + tokio::time::Duration::from_secs(10);
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            anyhow::bail!("Timeout waiting for Computors response");
        }

        match tokio::time::timeout(remaining, read_packet(stream)).await {
            Ok(Ok((msg_type, _dejavu, payload))) => {
                if msg_type == 2 {
                    // RESPOND_COMPUTOR_LIST
                    return Ok(payload);
                }
                if msg_type == 35 || msg_type == 54 {
                    anyhow::bail!("Node error while waiting for Computors");
                }
                debug!("Skipping packet type={msg_type} while waiting for Computors");
            }
            Ok(Err(e)) => return Err(e).context("Read error waiting for Computors"),
            Err(_) => anyhow::bail!("Timeout waiting for Computors response"),
        }
    }
}

/// Request entity data (type 31).
pub async fn request_entity(stream: &mut TcpStream, identity: &[u8; 32]) -> Result<Vec<u8>> {
    let dejavu = rand::random::<u32>().max(1);
    send_raw(stream, 31, identity, dejavu).await?;

    let deadline = tokio::time::Instant::now() + tokio::time::Duration::from_secs(10);
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            anyhow::bail!("Timeout waiting for Entity response");
        }

        match tokio::time::timeout(remaining, read_packet(stream)).await {
            Ok(Ok((msg_type, _dejavu, payload))) => {
                if msg_type == 32 {
                    // RESPOND_ENTITY
                    return Ok(payload);
                }
                if msg_type == 35 || msg_type == 54 {
                    anyhow::bail!("Node error while waiting for Entity");
                }
                debug!("Skipping packet type={msg_type} while waiting for Entity");
            }
            Ok(Err(e)) => return Err(e).context("Read error waiting for Entity"),
            Err(_) => anyhow::bail!("Timeout waiting for Entity response"),
        }
    }
}
