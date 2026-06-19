//! Packet decoder: dispatches decoded packets to appropriate NATS subjects.
//!
//! Each message type has a dedicated decode function that parses the raw payload
//! into a typed struct, then publishes it as JSON to the corresponding NATS subject.

use anyhow::Result;
use async_nats::Client as NatsClient;
use tracing::{debug, warn};

use crate::decoders;

/// Decodes raw TCP packets and publishes to NATS.
pub struct PacketDecoder {
    // Placeholder for epoch tracking
    _current_epoch: u16,
}

impl Default for PacketDecoder {
    fn default() -> Self {
        Self::new()
    }
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
        let tick = decoders::decode_tick(payload)?;
        debug!("Decoded tick: {tick:?}");
        // TODO: publish to NATS subject SUBJECT_TICK
        Ok(())
    }

    async fn decode_transaction(&self, payload: &[u8], _nats: &NatsClient) -> Result<()> {
        let tx = decoders::decode_transaction(payload)?;
        debug!("Decoded transaction: hash={} type={} amount={}", tx.hash, tx.tx_type, tx.amount);
        // TODO: publish to NATS subject SUBJECT_TX
        Ok(())
    }

    async fn decode_computors(&self, payload: &[u8], _nats: &NatsClient) -> Result<()> {
        let computors = decoders::decode_computors(payload)?;
        debug!(
            "Decoded computors: epoch={}, keys={}",
            computors.epoch,
            computors.public_keys.len()
        );
        // TODO: publish to NATS subject SUBJECT_COMPUTORS
        Ok(())
    }

    async fn decode_entity(
        &self,
        payload: &[u8],
        _dejavu: u32,
        _nats: &NatsClient,
    ) -> Result<()> {
        let entity = decoders::decode_entity(payload)?;
        debug!(
            "Decoded entity: identity={} incoming={} outgoing={}",
            entity.identity, entity.incoming, entity.outgoing
        );
        // TODO: publish to NATS subject SUBJECT_ENTITY
        Ok(())
    }

    async fn decode_contract_ipo(
        &self,
        payload: &[u8],
        dejavu: u32,
        _nats: &NatsClient,
    ) -> Result<()> {
        let ipo = decoders::decode_contract_ipo(payload, dejavu)?;
        debug!(
            "Decoded contract IPO: contract_index={}, bids={}",
            ipo.contract_index,
            ipo.bids.len()
        );
        // TODO: publish to NATS subject SUBJECT_CONTRACT
        Ok(())
    }

    async fn decode_system_info(&self, payload: &[u8], _nats: &NatsClient) -> Result<()> {
        let info = decoders::decode_system_info(payload)?;
        debug!(
            "Decoded system info: version={} tick={} peers={}",
            info.version, info.current_tick, info.peer_count
        );
        // TODO: publish to NATS or use for internal state
        Ok(())
    }

    async fn decode_current_tick_info(
        &self,
        payload: &[u8],
        _nats: &NatsClient,
    ) -> Result<()> {
        let info = decoders::decode_current_tick_info(payload)?;
        debug!(
            "Decoded current tick info: epoch={} tick={} aligned={} misaligned={}",
            info.epoch, info.tick, info.number_of_aligned_votes, info.number_of_misaligned_votes
        );
        // TODO: publish to NATS or use for internal state
        Ok(())
    }

    async fn decode_contract_function(
        &self,
        _payload: &[u8],
        _dejavu: u32,
        _nats: &NatsClient,
    ) -> Result<()> {
        // TODO: Implement contract function response decoding
        debug!("Received contract function response (decoder not yet implemented)");
        Ok(())
    }

    async fn decode_broadcast_message(
        &self,
        payload: &[u8],
        _nats: &NatsClient,
    ) -> Result<()> {
        let events = decoders::decode_log_events(payload)?;
        debug!("Decoded {} log events from broadcast message", events.len());
        // TODO: publish each event to NATS subject SUBJECT_CUSTOM_MESSAGE
        Ok(())
    }
}
