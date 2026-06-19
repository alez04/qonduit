//! Wire format decoders: raw bytes -> typed structs.
//!
//! All decoders validate minimum payload size and extract fields
//! at the correct byte offsets matching the C++ struct layouts.

use anyhow::Result;
use qonduit_core::identity::encode_base26;
use qonduit_core::*;

/// Decode a BroadcastTick payload (1708 bytes).
pub fn decode_tick(payload: &[u8]) -> Result<TickData> {
    if payload.len() < 1708 {
        anyhow::bail!("Tick payload too small: {} < 1708", payload.len());
    }

    let epoch = u16::from_le_bytes([payload[0], payload[1]]);
    let tick = u32::from_le_bytes([payload[2], payload[3], payload[4], payload[5]]);

    // Timestamp at offset 8
    let timestamp = u64::from_le_bytes([
        payload[8], payload[9], payload[10], payload[11], payload[12], payload[13], payload[14],
        payload[15],
    ]);

    // time_lock at offset 16 (32 bytes)
    let mut time_lock = [0u8; 32];
    time_lock.copy_from_slice(&payload[16..48]);

    // mining_nonce at offset 48
    let mining_nonce = u32::from_le_bytes([payload[48], payload[49], payload[50], payload[51]]);

    // salted hashes at offsets 52, 84, 116
    let mut salted_spectrum_hash = [0u8; 32];
    salted_spectrum_hash.copy_from_slice(&payload[52..84]);

    let mut salted_universe_hash = [0u8; 32];
    salted_universe_hash.copy_from_slice(&payload[84..116]);

    let mut salted_computor_hash = [0u8; 32];
    salted_computor_hash.copy_from_slice(&payload[116..148]);

    // transaction_count at offset 148
    let transaction_count = u16::from_le_bytes([payload[148], payload[149]]);

    // contract_counters at offset 150, up to 1024 entries (2048 bytes)
    // but limited by payload size (1708 bytes)
    let mut contract_counters = Vec::new();
    let counters_start = 150;
    let max_counters = (1708usize.saturating_sub(counters_start)) / 2;
    let num_counters = max_counters.min(MAX_NUMBER_OF_CONTRACTS);
    for i in 0..num_counters {
        let offset = counters_start + i * 2;
        if offset + 2 <= 1708 {
            contract_counters.push(u16::from_le_bytes([payload[offset], payload[offset + 1]]));
        }
    }

    // Signature count: computors whose vote signatures are present
    // In the broadcast, signatures are appended after the tick data
    // For now, compute from remaining payload after the fixed fields
    let sig_area_start = 1708;
    let sig_count = if payload.len() > sig_area_start {
        (payload.len() - sig_area_start) / SIGNATURE_SIZE
    } else {
        0
    };

    Ok(TickData {
        epoch,
        tick,
        timestamp,
        time_lock,
        mining_nonce,
        salted_spectrum_hash,
        salted_universe_hash,
        salted_computor_hash,
        transaction_count,
        contract_counters,
        signature_count: sig_count as u32,
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

    // Hash: hex of the transaction header + input (without signature)
    let hash = if payload.len() >= input_end {
        hex::encode(&payload[..input_end])
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

    let public_key_identities: Vec<String> = public_keys.iter().map(|k| encode_base26(k)).collect();

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

/// Decode RespondCurrentTickInfo payload (14+ bytes).
pub fn decode_current_tick_info(payload: &[u8]) -> Result<CurrentTickInfo> {
    if payload.len() < 14 {
        anyhow::bail!(
            "CurrentTickInfo payload too small: {} < 14",
            payload.len()
        );
    }

    let epoch = u16::from_le_bytes([payload[0], payload[1]]);
    let tick = u32::from_le_bytes([payload[2], payload[3], payload[4], payload[5]]);
    let number_of_aligned_votes = u16::from_le_bytes([payload[6], payload[7]]);
    let number_of_misaligned_votes = u16::from_le_bytes([payload[8], payload[9]]);
    let initial_tick = u32::from_le_bytes([payload[10], payload[11], payload[12], payload[13]]);

    // These fields extend beyond 14 bytes in newer protocol versions
    let latest_voting_tick = if payload.len() >= 18 {
        u32::from_le_bytes([payload[14], payload[15], payload[16], payload[17]])
    } else {
        0
    };

    let time_since_last_voting_tick = if payload.len() >= 26 {
        u64::from_le_bytes([
            payload[18], payload[19], payload[20], payload[21], payload[22], payload[23],
            payload[24], payload[25],
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

    let tick = u32::from_le_bytes([payload[0], payload[1], payload[2], payload[3]]);
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
