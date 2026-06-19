//! Wire protocol operations for the Qubic TCP interface.
//!
//! Provides helpers for sending requests, reading responses, and performing
//! the initial peer exchange handshake with a Qubic node.

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use anyhow::Result;
use tracing::info;

use qonduit_core::RequestResponseHeader;

/// Send a request packet and return the raw response payloads.
///
/// Builds the 8-byte header, writes the header + payload, then reads
/// response packets until an `EndResponse` or `TryAgain` is received.
pub async fn send_request(
    stream: &mut TcpStream,
    msg_type: u8,
    payload: &[u8],
    dejavu: u32,
) -> Result<Vec<Vec<u8>>> {
    // Build header
    let header = RequestResponseHeader::new_request(msg_type, payload.len() as u32, dejavu);
    // Write header bytes
    let header_bytes: [u8; 8] = unsafe { std::mem::transmute(header) };
    stream.write_all(&header_bytes).await?;
    // Write payload if non-empty
    if !payload.is_empty() {
        stream.write_all(payload).await?;
    }
    stream.flush().await?;

    // Read responses until END_RESPONSE or TryAgain
    let mut responses = Vec::new();
    loop {
        let mut hdr = [0u8; 8];
        stream.read_exact(&mut hdr).await?;
        let resp_header: &RequestResponseHeader =
            unsafe { &*(&hdr as *const [u8; 8] as *const RequestResponseHeader) };

        if resp_header.is_end_response() {
            break;
        }
        if resp_header.msg_type() == 54 {
            // TryAgain - bail
            anyhow::bail!("Node returned TryAgain");
        }

        let payload_size = resp_header.payload_size() as usize;
        if payload_size > 0 {
            let mut buf = vec![0u8; payload_size];
            stream.read_exact(&mut buf).await?;
            responses.push(buf);
        }
    }
    Ok(responses)
}

/// Perform the initial peer exchange handshake.
///
/// This is the first thing done after connecting. Sends our local peers
/// (type 0, 16 bytes = 4 x 4-byte IPv4 addresses) and reads theirs.
pub async fn exchange_public_peers(
    stream: &mut TcpStream,
    local_peers: &[[u8; 4]; 4],
) -> Result<[[u8; 4]; 4]> {
    // Send our peers (type 0, 16 bytes payload = 4 * 4-byte IPv4 addresses)
    let mut payload = Vec::with_capacity(16);
    for peer in local_peers {
        payload.extend_from_slice(peer);
    }

    let header = RequestResponseHeader::new_request(0, 16, 0);
    let header_bytes: [u8; 8] = unsafe { std::mem::transmute(header) };
    stream.write_all(&header_bytes).await?;
    stream.write_all(&payload).await?;
    stream.flush().await?;

    // Read their peers
    let mut resp_hdr = [0u8; 8];
    stream.read_exact(&mut resp_hdr).await?;
    let resp: &RequestResponseHeader =
        unsafe { &*(&resp_hdr as *const [u8; 8] as *const RequestResponseHeader) };

    let resp_size = resp.payload_size() as usize;
    let mut resp_payload = vec![0u8; resp_size];
    stream.read_exact(&mut resp_payload).await?;

    // Parse 4 IPv4 addresses
    let mut peers = [[0u8; 4]; 4];
    for i in 0..4 {
        let start = i * 4;
        if start + 4 <= resp_payload.len() {
            peers[i].copy_from_slice(&resp_payload[start..start + 4]);
        }
    }

    info!("Received {} bytes of peer data", resp_size);
    Ok(peers)
}

/// Request current tick info (type 27, no payload).
///
/// Returns the raw response bytes. The first 2 bytes are the epoch (u16 LE)
/// and bytes 2..6 are the tick (u32 LE).
pub async fn request_current_tick_info(stream: &mut TcpStream) -> Result<Vec<u8>> {
    let responses = send_request(stream, 27, &[], rand::random()).await?;
    responses
        .into_iter()
        .next()
        .ok_or_else(|| anyhow::anyhow!("No response for CurrentTickInfo"))
}

/// Request computors list (type 11, no payload).
pub async fn request_computors(stream: &mut TcpStream) -> Result<Vec<u8>> {
    let responses = send_request(stream, 11, &[], rand::random()).await?;
    responses
        .into_iter()
        .next()
        .ok_or_else(|| anyhow::anyhow!("No response for Computors"))
}

/// Request entity data (type 31).
pub async fn request_entity(stream: &mut TcpStream, identity: &[u8; 32]) -> Result<Vec<u8>> {
    let responses = send_request(stream, 31, identity, rand::random()).await?;
    responses
        .into_iter()
        .next()
        .ok_or_else(|| anyhow::anyhow!("No response for Entity"))
}
