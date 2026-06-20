//! Packet decoder: dispatches decoded packets to appropriate NATS subjects.
//!
//! Each message type has a dedicated decode function that parses the raw payload
//! into a typed struct, then publishes it as JSON to the corresponding NATS subject.

use std::collections::{HashMap, HashSet};
use std::sync::Mutex;

use anyhow::Result;
use qonduit_core::QUORUM;
use tracing::{debug, info, warn};

use crate::decoders;
use crate::metrics::{PACKETS_BY_TYPE, PACKETS_PUBLISHED, PACKETS_RECEIVED};
use crate::nats_publish::{CustomMessage, NatsPublisher, TickVote};

/// Tracks tick votes per (epoch, tick) for quorum detection.
struct TickVoteAggregator {
    /// Map of (epoch, tick) -> set of computor indices that voted.
    votes: HashMap<(u16, u32), HashSet<u16>>,
    /// Set of (epoch, tick) where quorum has already been published
    /// (to avoid duplicate quorum events).
    quorum_published: HashSet<(u16, u32)>,
}

impl TickVoteAggregator {
    fn new() -> Self {
        Self {
            votes: HashMap::new(),
            quorum_published: HashSet::new(),
        }
    }

    /// Record a vote. Returns `Some(quorum_tick)` if quorum is reached
    /// and hasn't been published yet.
    fn record_vote(
        &mut self,
        epoch: u16,
        tick: u32,
        computor_index: u16,
    ) -> Option<qonduit_core::QuorumTick> {
        let key = (epoch, tick);
        let voted = self.votes.entry(key).or_default();
        voted.insert(computor_index);

        if voted.len() >= QUORUM && !self.quorum_published.contains(&key) {
            self.quorum_published.insert(key);
            let mut computors: Vec<u16> = voted.iter().copied().collect();
            computors.sort_unstable();
            Some(qonduit_core::QuorumTick {
                epoch,
                tick,
                vote_count: voted.len() as u16,
                voted_computors: computors,
            })
        } else {
            None
        }
    }

    /// Periodically prune old entries to bound memory.
    /// Removes entries for ticks more than `keep_ticks` behind `latest_tick`.
    fn prune(&mut self, latest_tick: u32, keep_ticks: u32) {
        if latest_tick > keep_ticks {
            let cutoff = latest_tick - keep_ticks;
            self.votes.retain(|&(_, tick), _| tick >= cutoff);
            self.quorum_published.retain(|&(_, tick)| tick >= cutoff);
        }
    }
}

/// Decodes raw TCP packets and publishes to NATS.
pub struct PacketDecoder {
    publisher: NatsPublisher,
    vote_aggregator: Mutex<TickVoteAggregator>,
    /// Counter for periodic pruning (prune every N votes).
    vote_count: Mutex<u64>,
}

impl PacketDecoder {
    /// How often to prune the vote aggregator (every N votes).
    const PRUNE_INTERVAL: u64 = 10_000;
    /// How many ticks of history to keep in the aggregator.
    const KEEP_TICKS: u32 = 256;

    pub fn new(nats: async_nats::Client, fire_and_forget: bool) -> Self {
        let mut publisher = NatsPublisher::new(nats);
        publisher.set_fire_and_forget(fire_and_forget);
        Self {
            publisher,
            vote_aggregator: Mutex::new(TickVoteAggregator::new()),
            vote_count: Mutex::new(0),
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
            // RespondAllLogIdRangesFromTick (type 51)
            51 => {
                self.decode_all_log_id_ranges(payload, dejavu).await?;
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
            // RespondPruningLog (type 57)
            57 => {
                self.decode_pruning_log(payload).await?;
            }
            // RespondLogStateDigest (type 59)
            59 => {
                self.decode_log_state_digest(payload).await?;
            }
            // RespondOracleData (type 67)
            67 => {
                self.decode_oracle_data(payload, current_epoch).await?;
            }
            // BroadcastCustomMiningTask (type 68)
            68 => {
                self.decode_custom_mining_task(payload, current_epoch).await?;
            }
            // BroadcastCustomMiningSolution (type 69)
            69 => {
                self.decode_custom_mining_solution(payload, current_epoch).await?;
            }
            // RespondTxStatus (type 202, tx addon only)
            202 => {
                self.decode_tx_status(payload, dejavu).await?;
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

        // Publish individual vote to TICKVOTE stream
        let vote = TickVote {
            computor_index,
            epoch,
            tick,
        };
        self.publisher
            .publish_tick_vote(current_epoch, &vote)
            .await?;

        // Aggregate vote and check for quorum
        let quorum_tick = {
            let mut agg = self.vote_aggregator.lock().unwrap();
            let result = agg.record_vote(epoch, tick, computor_index);

            // Periodic pruning
            let mut count = self.vote_count.lock().unwrap();
            *count += 1;
            if (*count).is_multiple_of(Self::PRUNE_INTERVAL) {
                agg.prune(tick, Self::KEEP_TICKS);
            }

            result
        };

        if let Some(qt) = quorum_tick {
            info!(
                "QUORUM reached for tick {} epoch {}: {} votes",
                qt.tick, qt.epoch, qt.vote_count
            );
            self.publisher
                .publish_quorum_tick(current_epoch, &qt)
                .await?;
        }

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
            tx.hash, tx.input_type_name, tx.amount
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
        _dejavu: u32,
        current_epoch: u16,
    ) -> Result<()> {
        let ipo = decoders::decode_contract_ipo(payload)?;
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
            "Decoded system info: version={} tick={}",
            info.version, info.tick
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

        // Publish raw log events to the dedicated log stream for indexing
        if !events.is_empty() {
            self.publisher
                .publish_log_events(current_epoch, &events)
                .await?;
        }

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

    /// Decode RespondPruningLog (type 57) and log the result.
    async fn decode_pruning_log(&self, payload: &[u8]) -> Result<()> {
        let resp = decoders::decode_pruning_log_response(payload)?;
        if resp.success == 0 {
            debug!("Log pruning succeeded");
        } else {
            warn!("Log pruning failed with error code: {}", resp.success);
        }
        Ok(())
    }

    /// Decode RespondLogStateDigest (type 59) and publish.
    async fn decode_log_state_digest(&self, payload: &[u8]) -> Result<()> {
        let digest = decoders::decode_log_state_digest(payload)?;
        debug!("Decoded log state digest: {}", digest.digest_hex);
        self.publisher
            .publish_log_state_digest(&digest)
            .await?;
        Ok(())
    }

    /// Decode RespondOracleData (type 67) and publish.
    async fn decode_oracle_data(&self, payload: &[u8], current_epoch: u16) -> Result<()> {
        let oracle = decoders::decode_oracle_data(payload)?;
        debug!(
            "Decoded oracle data: resType={}, payload_len={}",
            oracle.res_type,
            oracle.payload_hex.len() / 2
        );
        self.publisher
            .publish_oracle_data(current_epoch, &oracle)
            .await?;
        Ok(())
    }

    /// Decode BroadcastCustomMiningTask (type 68) and publish.
    async fn decode_custom_mining_task(&self, payload: &[u8], current_epoch: u16) -> Result<()> {
        let task = decoders::decode_custom_mining_task(payload)?;
        debug!(
            "Decoded custom mining task: job_id={}, mining_type={}",
            task.job_id, task.custom_mining_type
        );
        self.publisher
            .publish_custom_mining_task(current_epoch, &task)
            .await?;
        Ok(())
    }

    /// Decode BroadcastCustomMiningSolution (type 69) and publish.
    async fn decode_custom_mining_solution(
        &self,
        payload: &[u8],
        current_epoch: u16,
    ) -> Result<()> {
        let solution = decoders::decode_custom_mining_solution(payload)?;
        debug!(
            "Decoded custom mining solution: job_id={}, mining_type={}",
            solution.job_id, solution.custom_mining_type
        );
        self.publisher
            .publish_custom_mining_solution(current_epoch, &solution)
            .await?;
        Ok(())
    }

    /// Decode RespondTxStatus (type 202) and log the result.
    async fn decode_tx_status(&self, payload: &[u8], _dejavu: u32) -> Result<()> {
        // The dejavu field for respond messages carries the tick from the request context.
        // We use dejavu as a placeholder; the actual tick is inside the payload.
        let resp = decoders::decode_tx_status(payload, 0)?;
        debug!(
            "Decoded tx status: tick={} node_tick={} tx_count={}",
            resp.tick, resp.current_tick_of_node, resp.tx_count
        );
        // Tx status is an on-demand response, not broadcast. Log only.
        Ok(())
    }

    /// Decode RespondAllLogIdRangesFromTick (type 51) and log the result.
    async fn decode_all_log_id_ranges(&self, payload: &[u8], _dejavu: u32) -> Result<()> {
        // The tick is extracted from the request context; here we pass 0 as placeholder.
        let resp = decoders::decode_all_log_id_ranges(payload, 0)?;
        debug!(
            "Decoded log id ranges: tick={} entries={}",
            resp.tick, resp.tx_count
        );
        // Log ranges are on-demand responses. Log only.
        Ok(())
    }
}
