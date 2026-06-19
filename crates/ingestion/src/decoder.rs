//! Packet decoder: dispatches decoded packets to appropriate NATS subjects.
//!
//! Each message type has a dedicated decode function that parses the raw payload
//! into a typed struct, then publishes it as JSON to the corresponding NATS subject.

use anyhow::Result;
use async_nats::Client as NatsClient;
use tracing::{debug, warn};

use crate::nats_publish::NatsPublisher;

/// Decodes raw TCP packets and publishes to NATS.
pub struct PacketDecoder {
    publisher: NatsPublisher,
    _current_epoch: u16,
}

impl PacketDecoder {
    pub fn new(publisher: NatsPublisher) -> Self {
        Self {
            publisher,
            _current_epoch: 0,
        }
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
        let tick_num = u32::from_le_bytes([payload[2], payload[3], payload[4], payload[5]]);
        let timestamp = u64::from_le_bytes(payload[4112..4120].try_into().unwrap());
        let time_lock: [u8; 32] = payload[4120..4152].try_into().unwrap();
        let mining_nonce = u32::from_le_bytes(payload[8248..8252].try_into().unwrap());
        let salted_spectrum_hash: [u8; 32] = payload[6200..6232].try_into().unwrap();
        let salted_universe_hash: [u8; 32] = payload[6232..6264].try_into().unwrap();
        let salted_computor_hash: [u8; 32] = payload[6264..6296].try_into().unwrap();

        // Count signatures (non-zero 64-byte blocks)
        let mut signature_count = 0u32;
        for i in 0..676 {
            let offset = 6 + i * 64;
            let sig = &payload[offset..offset + 64];
            if sig.iter().any(|&b| b != 0) {
                signature_count += 1;
            }
        }

        // Total transaction count from first slot
        let transaction_count = u16::from_le_bytes([payload[4152], payload[4153]]);

        let tick_data = qonduit_core::TickData {
            epoch,
            tick: tick_num,
            timestamp,
            time_lock,
            mining_nonce,
            salted_spectrum_hash,
            salted_universe_hash,
            salted_computor_hash,
            transaction_count,
            contract_counters: Vec::new(),
            signature_count,
        };

        self.publisher.publish_tick(epoch, &tick_data).await?;
        debug!("Tick: epoch={epoch}, tick={tick_num}");
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
        let epoch = u16::from_le_bytes([payload[0], payload[1]]);

        let mut public_keys = Vec::with_capacity(676);
        for i in 0..676 {
            let offset = 2 + i * 32;
            let key: [u8; 32] = payload[offset..offset + 32].try_into().unwrap();
            public_keys.push(key);
        }

        let computors = qonduit_core::Computors {
            epoch,
            public_keys,
            public_key_identities: Vec::new(),
        };

        self.publisher.publish_computors(epoch, &computors).await?;
        debug!("Computors: epoch={epoch}");
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
