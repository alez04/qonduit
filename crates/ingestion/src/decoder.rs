//! Packet decoder: dispatches decoded packets to appropriate NATS subjects.
//!
//! Each message type has a dedicated decode function that parses the raw payload
//! into a typed struct, then publishes it as JSON to the corresponding NATS subject.

use anyhow::Result;
use tracing::{debug, warn};

use crate::decoders;
use crate::metrics::{PACKETS_BY_TYPE, PACKETS_PUBLISHED, PACKETS_RECEIVED};
use crate::nats_publish::{CustomMessage, NatsPublisher, TickVote};

/// Decodes raw TCP packets and publishes to NATS.
pub struct PacketDecoder {
    publisher: NatsPublisher,
}

impl PacketDecoder {
    pub fn new(nats: async_nats::Client) -> Self {
        Self {
            publisher: NatsPublisher::new(nats),
        }
    }

    /// Decode a packet and publish to the appropriate NATS subject.
    pub async fn decode_and_publish(
        &self,
        msg_type: u8,
        dejavu: u32,
        payload: &[u8],
        current_epoch: u16,
    ) -> Result<()> {
        PACKETS_RECEIVED.inc();
        PACKETS_BY_TYPE
            .with_label_values(&[&msg_type.to_string()])
            .inc();

        match msg_type {
            // BroadcastTickVote (type 3) — individual computor vote
            3 => {
                self.decode_tick_vote(payload, current_epoch).await?;
            }
            // BroadcastTickData (type 8) — full tick data from computor
            8 => {
                self.decode_tick(payload, current_epoch).await?;
            }
            // BroadcastTransaction (type 24)
            24 => {
                self.decode_transaction(payload, current_epoch).await?;
            }
            // BroadcastComputors (type 2)
            2 => {
                self.decode_computors(payload, current_epoch).await?;
            }
            // RespondEntity (type 32)
            32 => {
                self.decode_entity(payload, dejavu, current_epoch).await?;
            }
            // RespondContractIpo (type 34)
            34 => {
                self.decode_contract_ipo(payload, dejavu, current_epoch).await?;
            }
            // RespondSystemInfo (type 47)
            47 => {
                self.decode_system_info(payload).await?;
            }
            // RespondCurrentTickInfo (type 28)
            28 => {
                self.decode_current_tick_info(payload).await?;
            }
            // RespondContractFunction (type 43)
            43 => {
                self.decode_contract_function(payload, dejavu, current_epoch).await?;
            }
            // TryAgain (type 54)
            54 => {
                warn!("Node requested TryAgain");
            }
            // EndResponse (type 35) - no-op
            35 => {}
            // BroadcastMessage (type 1) - log events
            1 => {
                self.decode_broadcast_message(payload, current_epoch).await?;
            }
            _ => {
                debug!("Unhandled message type: {msg_type}");
            }
        }
        PACKETS_PUBLISHED.inc();
        Ok(())
    }

    async fn decode_tick_vote(&self, payload: &[u8], current_epoch: u16) -> Result<()> {
        if payload.len() < 20 {
            warn!("TickVote payload too small: {} bytes", payload.len());
            return Ok(());
        }
        let computor_index = u16::from_le_bytes([payload[0], payload[1]]);
        let epoch = u16::from_le_bytes([payload[2], payload[3]]);
        let tick = u32::from_le_bytes([payload[4], payload[5], payload[6], payload[7]]);
        debug!(
            "TickVote: computor={computor_index}, epoch={epoch}, tick={tick}"
        );
        let vote = TickVote {
            computor_index,
            epoch,
            tick,
        };
        self.publisher
            .publish_tick_vote(current_epoch, &vote)
            .await?;
        Ok(())
    }

    async fn decode_tick(&self, payload: &[u8], current_epoch: u16) -> Result<()> {
        let tick = decoders::decode_tick(payload)?;
        debug!("Decoded tick: {tick:?}");
        self.publisher.publish_tick(current_epoch, &tick).await?;
        Ok(())
    }

    async fn decode_transaction(&self, payload: &[u8], current_epoch: u16) -> Result<()> {
        let tx = decoders::decode_transaction(payload)?;
        debug!(
            "Decoded transaction: hash={} type={} amount={}",
            tx.hash, tx.tx_type, tx.amount
        );
        self.publisher.publish_tx(current_epoch, &tx).await?;
        Ok(())
    }

    async fn decode_computors(&self, payload: &[u8], current_epoch: u16) -> Result<()> {
        let computors = decoders::decode_computors(payload)?;
        debug!(
            "Decoded computors: epoch={}, keys={}",
            computors.epoch,
            computors.public_keys.len()
        );
        self.publisher
            .publish_computors(current_epoch, &computors)
            .await?;
        Ok(())
    }

    async fn decode_entity(
        &self,
        payload: &[u8],
        _dejavu: u32,
        current_epoch: u16,
    ) -> Result<()> {
        let entity = decoders::decode_entity(payload)?;
        debug!(
            "Decoded entity: identity={} incoming={} outgoing={}",
            entity.identity, entity.incoming, entity.outgoing
        );
        self.publisher
            .publish_entity(current_epoch, &entity)
            .await?;
        Ok(())
    }

    async fn decode_contract_ipo(
        &self,
        payload: &[u8],
        dejavu: u32,
        current_epoch: u16,
    ) -> Result<()> {
        let ipo = decoders::decode_contract_ipo(payload, dejavu)?;
        debug!(
            "Decoded contract IPO: contract_index={}, bids={}",
            ipo.contract_index,
            ipo.bids.len()
        );
        self.publisher
            .publish_contract_ipo(current_epoch, &ipo)
            .await?;
        Ok(())
    }

    async fn decode_system_info(&self, payload: &[u8]) -> Result<()> {
        let info = decoders::decode_system_info(payload)?;
        debug!(
            "Decoded system info: version={} tick={} peers={}",
            info.version, info.current_tick, info.peer_count
        );
        // System info is used for internal state tracking, no NATS publishing needed.
        Ok(())
    }

    async fn decode_current_tick_info(&self, payload: &[u8]) -> Result<()> {
        let info = decoders::decode_current_tick_info(payload)?;
        debug!(
            "Decoded current tick info: epoch={} tick={} aligned={} misaligned={}",
            info.epoch,
            info.tick,
            info.number_of_aligned_votes,
            info.number_of_misaligned_votes
        );
        // Current tick info is used for internal state tracking, no NATS publishing needed.
        Ok(())
    }

    async fn decode_contract_function(
        &self,
        payload: &[u8],
        dejavu: u32,
        current_epoch: u16,
    ) -> Result<()> {
        debug!("Received contract function response, publishing raw data");
        self.publisher
            .publish_contract_fn(current_epoch, dejavu, payload)
            .await?;
        Ok(())
    }

    async fn decode_broadcast_message(
        &self,
        payload: &[u8],
        current_epoch: u16,
    ) -> Result<()> {
        let events = decoders::decode_log_events(payload)?;
        debug!("Decoded {} log events from broadcast message", events.len());
        for event in &events {
            let msg = CustomMessage {
                tick: event.tick,
                message_type: event.event_type as u32,
                payload_hex: hex::encode(&event.data),
            };
            self.publisher
                .publish_custom_message(current_epoch, event.tick, &msg)
                .await?;
        }
        Ok(())
    }
}
