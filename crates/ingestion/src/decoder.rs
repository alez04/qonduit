//! Packet decoder: dispatches decoded packets to appropriate NATS subjects.
//!
//! Each message type has a dedicated decode function that parses the raw payload
//! into a typed struct, then publishes it as JSON to the corresponding NATS subject.

use anyhow::Result;
use async_nats::Client as NatsClient;
use tracing::{debug, warn};

/// Decodes raw TCP packets and publishes to NATS.
pub struct PacketDecoder {
    // Placeholder for epoch tracking
    _current_epoch: u16,
}

impl PacketDecoder {
    pub fn new() -> Self {
        Self { _current_epoch: 0 }
    }

    /// Decode a packet and publish to the appropriate NATS subject.
    pub async fn decode_and_publish(
        &self,
        msg_type: u8,
        dejavu: u32,
        payload: &[u8],
        nats: &NatsClient,
    ) -> Result<()> {
        match msg_type {
            // BroadcastTick (type 3)
            3 => {
                self.decode_tick(payload, nats).await?;
            }
            // BroadcastTransaction (type 24)
            24 => {
                self.decode_transaction(payload, nats).await?;
            }
            // BroadcastComputors (type 2)
            2 => {
                self.decode_computors(payload, nats).await?;
            }
            // RespondEntity (type 32)
            32 => {
                self.decode_entity(payload, dejavu, nats).await?;
            }
            // RespondContractIpo (type 34)
            34 => {
                self.decode_contract_ipo(payload, dejavu, nats).await?;
            }
            // RespondSystemInfo (type 47)
            47 => {
                self.decode_system_info(payload, nats).await?;
            }
            // RespondCurrentTickInfo (type 28)
            28 => {
                self.decode_current_tick_info(payload, nats).await?;
            }
            // RespondContractFunction (type 43)
            43 => {
                self.decode_contract_function(payload, dejavu, nats).await?;
            }
            // TryAgain (type 54)
            54 => {
                warn!("Node requested TryAgain");
            }
            // EndResponse (type 35) - no-op
            35 => {}
            // BroadcastMessage (type 1) - log events
            1 => {
                self.decode_broadcast_message(payload, nats).await?;
            }
            _ => {
                debug!("Unhandled message type: {msg_type}");
            }
        }
        Ok(())
    }

    async fn decode_tick(&self, payload: &[u8], _nats: &NatsClient) -> Result<()> {
        if payload.len() < 1708 {
            anyhow::bail!("Tick payload too small: {} < 1708", payload.len());
        }
        let epoch = u16::from_le_bytes([payload[0], payload[1]]);
        let tick = u32::from_le_bytes([payload[2], payload[3], payload[4], payload[5]]);
        debug!("Tick: epoch={epoch}, tick={tick}");
        // TODO: Full tick decode + publish to NATS
        Ok(())
    }

    async fn decode_transaction(&self, payload: &[u8], _nats: &NatsClient) -> Result<()> {
        if payload.len() < 80 {
            anyhow::bail!("Transaction payload too small: {} < 80", payload.len());
        }
        // TODO: Full transaction decode + publish to NATS
        Ok(())
    }

    async fn decode_computors(&self, payload: &[u8], _nats: &NatsClient) -> Result<()> {
        if payload.len() < 21626 {
            anyhow::bail!("Computors payload too small: {} < 21626", payload.len());
        }
        // TODO: Full computors decode + publish
        Ok(())
    }

    async fn decode_entity(
        &self,
        payload: &[u8],
        _dejavu: u32,
        _nats: &NatsClient,
    ) -> Result<()> {
        if payload.len() < 104 {
            anyhow::bail!("Entity payload too small: {} < 104", payload.len());
        }
        // TODO: Full entity decode + publish
        Ok(())
    }

    async fn decode_contract_ipo(
        &self,
        _payload: &[u8],
        _dejavu: u32,
        _nats: &NatsClient,
    ) -> Result<()> {
        // TODO: Decode contract IPO bids
        Ok(())
    }

    async fn decode_system_info(&self, payload: &[u8], _nats: &NatsClient) -> Result<()> {
        if payload.len() < 144 {
            anyhow::bail!("SystemInfo payload too small: {} < 144", payload.len());
        }
        // TODO: Decode system info + publish
        Ok(())
    }

    async fn decode_current_tick_info(
        &self,
        payload: &[u8],
        _nats: &NatsClient,
    ) -> Result<()> {
        if payload.len() < 14 {
            anyhow::bail!("CurrentTickInfo payload too small: {} < 14", payload.len());
        }
        // TODO: Decode + publish
        Ok(())
    }

    async fn decode_contract_function(
        &self,
        _payload: &[u8],
        _dejavu: u32,
        _nats: &NatsClient,
    ) -> Result<()> {
        // TODO: Decode response + publish
        Ok(())
    }

    async fn decode_broadcast_message(
        &self,
        _payload: &[u8],
        _nats: &NatsClient,
    ) -> Result<()> {
        // TODO: Decode log events and publish to appropriate subjects
        Ok(())
    }
}
