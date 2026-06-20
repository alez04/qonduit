//! Wire format decoders: raw bytes -> typed structs.
//!
//! All decoders validate minimum payload size and extract fields
//! at the correct byte offsets matching the C++ struct layouts.

use anyhow::Result;
use qonduit_core::identity::encode_base26;
use qonduit_core::*;

/// Decode a BroadcastFutureTickData payload (type 8).
///
/// Real C++ TickData layout (little-endian):
///
/// ```text
/// Offset  Size  Field
/// 0       2     computorIndex (u16)
/// 2       2     epoch (u16)
/// 4       4     tick (u32)
/// 8       2     millisecond (u16)
/// 10      1     second (u8)
/// 11      1     minute (u8)
/// 12      1     hour (u8)
/// 13      1     day (u8)
/// 14      1     month (u8)
/// 15      1     year (u8)
/// 16      32    timelock (m256i)
/// 48      ...   transactionDigests[4096] (32 bytes each)
/// ...     ...   contractFees[1024] (8 bytes each)
/// end     64    signature
/// ```
///
/// We extract the header fields and keep the rest as raw bytes.
pub fn decode_tick(payload: &[u8]) -> Result<TickData> {
    // Minimum: header (16 bytes) + timelock (32 bytes) + at least 1 signature (64 bytes)
    if payload.len() < 112 {
        anyhow::bail!(
            "TickData payload too small: {} < 112",
            payload.len()
        );
    }

    // --- Header fields ---
    let computor_index = u16::from_le_bytes([payload[0], payload[1]]);
    let epoch = u16::from_le_bytes([payload[2], payload[3]]);
    let tick = u32::from_le_bytes([payload[4], payload[5], payload[6], payload[7]]);

    // --- Timestamp fields (offset 8-15) ---
    let _millisecond = u16::from_le_bytes([payload[8], payload[9]]);
    let second = payload[10];
    let minute = payload[11];
    let hour = payload[12];
    let day = payload[13];
    let month = payload[14];
    let year = payload[15];

    // Reconstruct a rough timestamp from components
    let timestamp = ((year as u64) << 40)
        | ((month as u64) << 32)
        | ((day as u64) << 24)
        | ((hour as u64) << 16)
        | ((minute as u64) << 8)
        | (second as u64);

    // --- Timelock at offset 16 (32 bytes) ---
    let mut time_lock = [0u8; 32];
    time_lock.copy_from_slice(&payload[16..48]);

    Ok(TickData {
        computor_index,
        epoch,
        tick,
        timestamp,
        time_lock,
    })
}

/// Decode a BroadcastTransaction payload (80 + input_size + 64 bytes).
pub fn decode_transaction(payload: &[u8]) -> Result<Transaction> {
    if payload.len() < TX_HEADER_SIZE {
        anyhow::bail!(
            "Transaction payload too small: {} < {}",
            payload.len(),
            TX_HEADER_SIZE
        );
    }

    let tx_type = payload[0];

    let mut source = [0u8; 32];
    source.copy_from_slice(&payload[1..33]);

    let mut destination = [0u8; 32];
    destination.copy_from_slice(&payload[33..65]);

    let amount = i64::from_le_bytes([
        payload[65], payload[66], payload[67], payload[68], payload[69], payload[70], payload[71],
        payload[72],
    ]);

    let tick = u32::from_le_bytes([payload[73], payload[74], payload[75], payload[76]]);
    let input_size = u16::from_le_bytes([payload[77], payload[78]]);
    let input_type = u16::from_le_bytes([payload[79], payload[80]]);

    // Input payload starts at offset 80
    let input_end = TX_HEADER_SIZE + input_size as usize;
    let input_hex = if input_size > 0 && payload.len() >= input_end {
        hex::encode(&payload[TX_HEADER_SIZE..input_end])
    } else {
        String::new()
    };

    // Signature follows input payload
    let sig_start = input_end;
    let sig_end = sig_start + SIGNATURE_SIZE;
    let signature_hex = if payload.len() >= sig_end {
        hex::encode(&payload[sig_start..sig_end])
    } else {
        String::new()
    };

    // Hash: SHA-256 of the transaction header + input (without signature)
    let hash = if payload.len() >= input_end {
        let tx_hash = qonduit_core::compute_tx_hash(payload, input_size);
        qonduit_core::hash_to_hex(&tx_hash)
    } else {
        String::new()
    };

    Ok(Transaction {
        hash,
        tx_type,
        source_hex: hex::encode(source),
        source_identity: encode_base26(&source),
        destination_hex: hex::encode(destination),
        destination_identity: encode_base26(&destination),
        amount,
        tick,
        input_type,
        input_size,
        input_hex,
        signature_hex,
    })
}

/// Decode BroadcastComputors payload (21626 bytes).
///
/// Layout: 2 bytes epoch + 2 bytes padding + 676 * 32 bytes public keys + 64 bytes signature.
pub fn decode_computors(payload: &[u8]) -> Result<Computors> {
    if payload.len() < 21626 {
        anyhow::bail!(
            "Computors payload too small: {} < 21626",
            payload.len()
        );
    }

    // Epoch at offset 0
    let epoch = u16::from_le_bytes([payload[0], payload[1]]);

    // Public keys start after the epoch field (offset 4 with padding, or 2)
    // C++ struct: { u16 epoch; u8 signingComputors[...]; m256i computorsPublicKeys[676]; ... }
    // In the actual wire format, keys are at offset 4 (2-byte epoch + 2-byte padding)
    let keys_start = 4;
    let mut public_keys = Vec::with_capacity(NUMBER_OF_COMPUTORS);

    for i in 0..NUMBER_OF_COMPUTORS {
        let offset = keys_start + i * PUBLIC_KEY_SIZE;
        if offset + PUBLIC_KEY_SIZE <= payload.len() {
            let mut key = [0u8; 32];
            key.copy_from_slice(&payload[offset..offset + PUBLIC_KEY_SIZE]);
            public_keys.push(key);
        }
    }

    let public_key_identities: Vec<String> = public_keys.iter().map(encode_base26).collect();

    Ok(Computors {
        epoch,
        public_keys,
        public_key_identities,
    })
}

/// Decode RespondEntity payload (473 bytes).
pub fn decode_entity(payload: &[u8]) -> Result<EntityData> {
    if payload.len() < 64 {
        anyhow::bail!("Entity payload too small: {} < 64", payload.len());
    }

    // [0..32] public key
    let mut pub_key = [0u8; 32];
    pub_key.copy_from_slice(&payload[0..32]);

    let incoming = i64::from_le_bytes([
        payload[32], payload[33], payload[34], payload[35], payload[36], payload[37], payload[38],
        payload[39],
    ]);

    let outgoing = i64::from_le_bytes([
        payload[40], payload[41], payload[42], payload[43], payload[44], payload[45], payload[46],
        payload[47],
    ]);

    let number_of_outgoing_transfers = u64::from_le_bytes([
        payload[48], payload[49], payload[50], payload[51], payload[52], payload[53], payload[54],
        payload[55],
    ]);

    let latest_incoming_transfer_tick =
        u32::from_le_bytes([payload[56], payload[57], payload[58], payload[59]]);

    let latest_outgoing_transfer_tick =
        u32::from_le_bytes([payload[60], payload[61], payload[62], payload[63]]);

    Ok(EntityData {
        identity: encode_base26(&pub_key),
        incoming,
        outgoing,
        number_of_outgoing_transfers,
        latest_incoming_transfer_tick,
        latest_outgoing_transfer_tick,
    })
}

/// Decode RespondCurrentTickInfo payload (type 28, 14+ bytes).
///
/// C++ layout:
/// ```text
/// Offset  Size  Field
/// 0       2     tickDuration (u16)
/// 2       2     epoch (u16)
/// 4       4     tick (u32)
/// 8       2     numberOfAlignedVotes (u16)
/// 10      2     numberOfMisalignedVotes (u16)
/// 12      4     initialTick (u32)
/// 16      4     latestVotingTick (u32)        [optional]
/// 20      8     timeSinceLastVotingTick (u64) [optional]
/// ```
pub fn decode_current_tick_info(payload: &[u8]) -> Result<CurrentTickInfo> {
    if payload.len() < 14 {
        anyhow::bail!(
            "CurrentTickInfo payload too small: {} < 14",
            payload.len()
        );
    }

    let _tick_duration = u16::from_le_bytes([payload[0], payload[1]]);
    let epoch = u16::from_le_bytes([payload[2], payload[3]]);
    let tick = u32::from_le_bytes([payload[4], payload[5], payload[6], payload[7]]);
    let number_of_aligned_votes = u16::from_le_bytes([payload[8], payload[9]]);
    let number_of_misaligned_votes = u16::from_le_bytes([payload[10], payload[11]]);
    let initial_tick = u32::from_le_bytes([payload[12], payload[13], payload[14], payload[15]]);

    // These fields extend beyond 16 bytes in newer protocol versions
    let latest_voting_tick = if payload.len() >= 20 {
        u32::from_le_bytes([payload[16], payload[17], payload[18], payload[19]])
    } else {
        0
    };

    let time_since_last_voting_tick = if payload.len() >= 28 {
        u64::from_le_bytes([
            payload[20], payload[21], payload[22], payload[23], payload[24], payload[25],
            payload[26], payload[27],
        ])
    } else {
        0
    };

    Ok(CurrentTickInfo {
        epoch,
        tick,
        number_of_aligned_votes,
        number_of_misaligned_votes,
        initial_tick,
        latest_voting_tick,
        time_since_last_voting_tick,
    })
}

/// Decode RespondSystemInfo payload (144 bytes).
pub fn decode_system_info(payload: &[u8]) -> Result<SystemInfoReply> {
    if payload.len() < 144 {
        anyhow::bail!(
            "SystemInfo payload too small: {} < 144",
            payload.len()
        );
    }

    let version = u64::from_le_bytes(payload[0..8].try_into().unwrap());
    let system_time_start = u64::from_le_bytes(payload[8..16].try_into().unwrap());
    let system_time_end = u64::from_le_bytes(payload[16..24].try_into().unwrap());
    let peer_count = u64::from_le_bytes(payload[24..32].try_into().unwrap());
    let first_epoch_start_tick = u64::from_le_bytes(payload[32..40].try_into().unwrap());
    let last_epoch_start_tick = u64::from_le_bytes(payload[40..48].try_into().unwrap());
    let current_tick = u64::from_le_bytes(payload[48..56].try_into().unwrap());
    let last_computor_event_tick = u64::from_le_bytes(payload[56..64].try_into().unwrap());
    let last_tick_transaction_count = u64::from_le_bytes(payload[64..72].try_into().unwrap());
    let max_peer_count = u64::from_le_bytes(payload[72..80].try_into().unwrap());

    Ok(SystemInfoReply {
        version,
        system_time_start,
        system_time_end,
        peer_count,
        first_epoch_start_tick,
        last_epoch_start_tick,
        current_tick,
        last_computor_event_tick,
        last_tick_transaction_count,
        max_peer_count,
    })
}

/// Decode RespondContractIpo payload (21608 bytes = 676 * 32 + signature).
pub fn decode_contract_ipo(payload: &[u8], contract_index: u32) -> Result<ContractIpo> {
    if payload.len() < 21608 {
        anyhow::bail!(
            "ContractIpo payload too small: {} < 21608",
            payload.len()
        );
    }

    // Each computor's bid is 32 bytes (first 8 bytes = i64 bid amount, rest padding/identity)
    let mut bids = Vec::with_capacity(NUMBER_OF_COMPUTORS);
    for i in 0..NUMBER_OF_COMPUTORS {
        let offset = i * 32;
        let mut identity_bytes = [0u8; 32];
        identity_bytes.copy_from_slice(&payload[offset..offset + 32]);

        let bid_amount = i64::from_le_bytes(payload[offset..offset + 8].try_into().unwrap());

        bids.push(ContractBid {
            identity: encode_base26(&identity_bytes),
            bid_amount,
        });
    }

    Ok(ContractIpo {
        contract_index,
        bids,
    })
}

/// Decode a BroadcastMessage (type 1) payload into log events.
///
/// The message contains packed log events, each with a fixed-size header
/// followed by variable-length data depending on the event type.
pub fn decode_log_events(payload: &[u8]) -> Result<Vec<LogEvent>> {
    if payload.len() < 8 {
        anyhow::bail!(
            "BroadcastMessage payload too small: {} < 8",
            payload.len()
        );
    }

    let _tick = u32::from_le_bytes([payload[0], payload[1], payload[2], payload[3]]);
    // bytes 4-7 are padding or message index

    let mut events = Vec::new();
    let mut offset = 8;

    while offset + LOG_HEADER_SIZE <= payload.len() {
        // Log event header (26 bytes):
        // [0..4]   tick u32
        // [4..8]   tx index within tick u32
        // [8..10]  epoch u16
        // [10..18] amount i64
        // [18..19] event_type u8
        // [19..26] padding / reserved

        let event_tick = u32::from_le_bytes([
            payload[offset],
            payload[offset + 1],
            payload[offset + 2],
            payload[offset + 3],
        ]);
        let tx_index = u32::from_le_bytes([
            payload[offset + 4],
            payload[offset + 5],
            payload[offset + 6],
            payload[offset + 7],
        ]);
        let event_epoch = u16::from_le_bytes([payload[offset + 8], payload[offset + 9]]);
        let amount = i64::from_le_bytes([
            payload[offset + 10],
            payload[offset + 11],
            payload[offset + 12],
            payload[offset + 13],
            payload[offset + 14],
            payload[offset + 15],
            payload[offset + 16],
            payload[offset + 17],
        ]);
        let event_type = payload[offset + 18];

        // Variable-length data after the header
        let data_start = offset + LOG_HEADER_SIZE;

        // Determine payload size from event type
        let data_size = match event_type {
            LOG_QU_TRANSFER => 64,                      // source(32) + dest(32)
            LOG_ASSET_ISSUANCE => 816,                  // issuance struct
            LOG_ASSET_OWNERSHIP_CHANGE => 64,           // old_owner(32) + new_owner(32)
            LOG_ASSET_POSSESSION_CHANGE => 64,          // old_possessor(32) + new_possessor(32)
            LOG_CONTRACT_ERROR_MESSAGE..=LOG_CONTRACT_DEBUG_MESSAGE => {
                // Variable length: first 2 bytes are the string length
                if data_start + 2 <= payload.len() {
                    u16::from_le_bytes([payload[data_start], payload[data_start + 1]]) as usize + 2
                } else {
                    break;
                }
            }
            LOG_BURNING => 8,                           // burned amount (i64)
            LOG_DUST_BURNING => 8,                      // dust amount (i64)
            LOG_SPECTRUM_STATS => 224,                  // spectrum stats struct
            _ => {
                // Unknown event type; stop parsing to avoid misalignment
                break;
            }
        };

        let data_end = data_start + data_size;
        if data_end > payload.len() {
            break;
        }

        events.push(LogEvent {
            tick: event_tick,
            tx_index,
            epoch: event_epoch,
            amount,
            event_type,
            data: payload[data_start..data_end].to_vec(),
        });

        offset = data_end;
    }

    Ok(events)
}
