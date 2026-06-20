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

    // Wire layout matches C++ Transaction struct:
    // [0..32]   source (m256i)
    // [32..64]  destination (m256i)
    // [64..72]  amount (i64 LE)
    // [72..76]  tick (u32 LE)
    // [76..78]  input_type (u16 LE)
    // [78..80]  input_size (u16 LE)
    let mut source = [0u8; 32];
    source.copy_from_slice(&payload[0..32]);

    let mut destination = [0u8; 32];
    destination.copy_from_slice(&payload[32..64]);

    let amount = i64::from_le_bytes([
        payload[64], payload[65], payload[66], payload[67], payload[68], payload[69], payload[70],
        payload[71],
    ]);

    let tick = u32::from_le_bytes([payload[72], payload[73], payload[74], payload[75]]);
    let input_type = u16::from_le_bytes([payload[76], payload[77]]);
    let input_size = u16::from_le_bytes([payload[78], payload[79]]);

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

    let input_type_label = InputType::classify(input_type).label().to_string();

    Ok(Transaction {
        hash,
        source_hex: hex::encode(source),
        source_identity: encode_base26(&source),
        destination_hex: hex::encode(destination),
        destination_identity: encode_base26(&destination),
        amount,
        tick,
        input_type,
        input_size,
        input_hex,
        input_type_name: input_type_label,
        signature_hex,
    })
}

/// Decode BroadcastComputors payload.
///
/// Layout from C++ `Computors` struct (no padding):
/// ```text
/// [0..2]    epoch (u16 LE)
/// [2..21634] publicKeys[676] (32 bytes each)
/// [21634..21698] signature (64 bytes)
/// ```
/// Total payload: 21698 bytes.
pub fn decode_computors(payload: &[u8]) -> Result<Computors> {
    // Minimum: 2 (epoch) + 676*32 (keys) = 21634
    if payload.len() < 21634 {
        anyhow::bail!(
            "Computors payload too small: {} < 21634",
            payload.len()
        );
    }

    // Epoch at offset 0 (u16)
    let epoch = u16::from_le_bytes([payload[0], payload[1]]);

    // Public keys start at offset 2 (immediately after u16 epoch, no padding in C++)
    let keys_start = 2;
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

    let number_of_incoming_transfers = u32::from_le_bytes([
        payload[48], payload[49], payload[50], payload[51],
    ]);

    let number_of_outgoing_transfers = u32::from_le_bytes([
        payload[52], payload[53], payload[54], payload[55],
    ]);

    let latest_incoming_transfer_tick =
        u32::from_le_bytes([payload[56], payload[57], payload[58], payload[59]]);

    let latest_outgoing_transfer_tick =
        u32::from_le_bytes([payload[60], payload[61], payload[62], payload[63]]);

    Ok(EntityData {
        identity: encode_base26(&pub_key),
        incoming,
        outgoing,
        number_of_incoming_transfers,
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
    if payload.len() < 16 {
        anyhow::bail!(
            "CurrentTickInfo payload too small: {} < 16",
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

/// Decode RespondSystemInfo payload (128 bytes, matching C++ layout).
pub fn decode_system_info(payload: &[u8]) -> Result<SystemInfoReply> {
    if payload.len() < 128 {
        anyhow::bail!(
            "SystemInfo payload too small: {} < 128",
            payload.len()
        );
    }

    let version = i16::from_le_bytes([payload[0], payload[1]]);
    let epoch = u16::from_le_bytes([payload[2], payload[3]]);
    let tick = u32::from_le_bytes([payload[4], payload[5], payload[6], payload[7]]);
    let initial_tick = u32::from_le_bytes([payload[8], payload[9], payload[10], payload[11]]);
    let latest_created_tick = u32::from_le_bytes([payload[12], payload[13], payload[14], payload[15]]);
    let initial_millisecond = u16::from_le_bytes([payload[16], payload[17]]);
    let initial_second = payload[18];
    let initial_minute = payload[19];
    let initial_hour = payload[20];
    let initial_day = payload[21];
    let initial_month = payload[22];
    let initial_year = payload[23];
    let number_of_entities = u32::from_le_bytes([payload[24], payload[25], payload[26], payload[27]]);
    let number_of_transactions = u32::from_le_bytes([payload[28], payload[29], payload[30], payload[31]]);
    let random_mining_seed_hex = hex::encode(&payload[32..64]);
    let solution_threshold = i32::from_le_bytes([payload[64], payload[65], payload[66], payload[67]]);
    let total_spectrum_amount = u64::from_le_bytes(payload[68..76].try_into().unwrap());
    let current_entity_balance_dust_threshold = u64::from_le_bytes(payload[76..84].try_into().unwrap());
    let target_tick_vote_signature = u32::from_le_bytes([payload[84], payload[85], payload[86], payload[87]]);
    let computor_packet_signature = u64::from_le_bytes(payload[88..96].try_into().unwrap());
    let solution_additional_threshold = u64::from_le_bytes(payload[96..104].try_into().unwrap());

    Ok(SystemInfoReply {
        version,
        epoch,
        tick,
        initial_tick,
        latest_created_tick,
        initial_millisecond,
        initial_second,
        initial_minute,
        initial_hour,
        initial_day,
        initial_month,
        initial_year,
        number_of_entities,
        number_of_transactions,
        random_mining_seed_hex,
        solution_threshold,
        total_spectrum_amount,
        current_entity_balance_dust_threshold,
        target_tick_vote_signature,
        computor_packet_signature,
        solution_additional_threshold,
    })
}

/// Decode RespondContractIpo payload (type 34).
///
/// Layout from C++ `RespondContractIPO`:
/// ```text
/// [0..4]    contractIndex (u32)
/// [4..8]    tick (u32)
/// [8..21640] publicKeys[676] (32 bytes each)
/// [21640..27048] prices[676] (i64 each)
/// ```
/// Total payload: 27048 bytes.
pub fn decode_contract_ipo(payload: &[u8]) -> Result<ContractIpo> {
    // Minimum: 8 (header) + 676*32 (keys) + 676*8 (prices) = 27048
    if payload.len() < 27048 {
        anyhow::bail!(
            "ContractIpo payload too small: {} < 27048",
            payload.len()
        );
    }

    let contract_index = u32::from_le_bytes([payload[0], payload[1], payload[2], payload[3]]);

    // Skip contractIndex + tick (8 bytes)
    let keys_offset = 8;
    let prices_offset = 8 + NUMBER_OF_COMPUTORS * PUBLIC_KEY_SIZE; // 8 + 21632 = 21640

    let mut bids = Vec::with_capacity(NUMBER_OF_COMPUTORS);
    for i in 0..NUMBER_OF_COMPUTORS {
        let key_off = keys_offset + i * PUBLIC_KEY_SIZE;
        let mut identity_bytes = [0u8; 32];
        identity_bytes.copy_from_slice(&payload[key_off..key_off + PUBLIC_KEY_SIZE]);

        let price_off = prices_offset + i * 8;
        let bid_amount = i64::from_le_bytes(
            payload[price_off..price_off + 8].try_into().unwrap(),
        );

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

/// Decode RespondOracleData payload (type 67).
///
/// First 4 bytes: resType (u32 LE), followed by type-specific payload.
/// We decode the header and pass through the rest as hex.
pub fn decode_oracle_data(payload: &[u8]) -> Result<OracleDataResponse> {
    if payload.len() < 4 {
        anyhow::bail!(
            "OracleData payload too small: {} < 4",
            payload.len()
        );
    }

    let res_type = u32::from_le_bytes([payload[0], payload[1], payload[2], payload[3]]);
    let payload_hex = if payload.len() > 4 {
        hex::encode(&payload[4..])
    } else {
        String::new()
    };

    Ok(OracleDataResponse {
        res_type,
        payload_hex,
    })
}

/// Decode RespondOracleData with resType=1 (query metadata).
///
/// Expects the payload AFTER the 4-byte resType header (i.e., payload[4..]).
pub fn decode_oracle_query_metadata(payload: &[u8]) -> Result<OracleQueryMetadata> {
    if payload.len() < ORACLE_QUERY_METADATA_SIZE {
        anyhow::bail!(
            "OracleQueryMetadata payload too small: {} < {}",
            payload.len(),
            ORACLE_QUERY_METADATA_SIZE
        );
    }

    let query_id = i64::from_le_bytes([
        payload[0], payload[1], payload[2], payload[3],
        payload[4], payload[5], payload[6], payload[7],
    ]);
    let query_type = payload[8];
    let status = payload[9];
    let status_flags = u16::from_le_bytes([payload[10], payload[11]]);
    let query_tick = u32::from_le_bytes([payload[12], payload[13], payload[14], payload[15]]);

    let querying_entity_hex = hex::encode(&payload[16..48]);

    let timeout = u64::from_le_bytes([
        payload[48], payload[49], payload[50], payload[51],
        payload[52], payload[53], payload[54], payload[55],
    ]);
    let interface_index = u32::from_le_bytes([payload[56], payload[57], payload[58], payload[59]]);
    let subscription_id = i32::from_le_bytes([payload[60], payload[61], payload[62], payload[63]]);
    let reveal_tick = u32::from_le_bytes([payload[64], payload[65], payload[66], payload[67]]);
    let total_commits = u16::from_le_bytes([payload[68], payload[69]]);
    let agreeing_commits = u16::from_le_bytes([payload[70], payload[71]]);

    Ok(OracleQueryMetadata {
        query_id,
        query_type,
        status,
        status_flags,
        query_tick,
        querying_entity_hex,
        timeout,
        interface_index,
        subscription_id,
        reveal_tick,
        total_commits,
        agreeing_commits,
    })
}

/// Decode BroadcastCustomMiningTask payload (type 68).
///
/// Wire format from `broadcast_message.h` + `custom_mining.h`:
/// ```text
/// [0..32]    sourcePublicKey (m256i)
/// [32..64]   zero / padding (m256i)
/// [64..96]   gammingNonce (m256i)
/// [96..128]  codeFileTrailerDigest (m256i)
/// [128..160] dataFileTrailerDigest (m256i)
/// [160..168] jobId (u64 LE)
/// [168]      customMiningType (u8)
/// [169..]    task-specific payload
/// [end-64..] dispatcher signature
/// ```
pub fn decode_custom_mining_task(payload: &[u8]) -> Result<CustomMiningTask> {
    // Minimum: 160 (header) + 8 (jobId) + 1 (type) + 64 (signature) = 233
    if payload.len() < 233 {
        anyhow::bail!(
            "CustomMiningTask payload too small: {} < 233",
            payload.len()
        );
    }

    let source_public_key_hex = hex::encode(&payload[0..32]);
    // bytes 32..64 are zero/padding, skip
    // bytes 64..96 are gammingNonce, skip
    let code_file_trailer_digest_hex = hex::encode(&payload[96..128]);
    let data_file_trailer_digest_hex = hex::encode(&payload[128..160]);

    let job_id = u64::from_le_bytes([
        payload[160], payload[161], payload[162], payload[163],
        payload[164], payload[165], payload[166], payload[167],
    ]);
    let custom_mining_type = payload[168];

    // Task-specific payload: from offset 169 to end - 64 (signature)
    let sig_start = payload.len() - 64;
    let payload_hex = hex::encode(&payload[169..sig_start]);
    let signature_hex = hex::encode(&payload[sig_start..]);

    Ok(CustomMiningTask {
        source_public_key_hex,
        code_file_trailer_digest_hex,
        data_file_trailer_digest_hex,
        job_id,
        custom_mining_type,
        payload_hex,
        signature_hex,
    })
}

/// Decode BroadcastCustomMiningSolution payload (type 69).
///
/// Wire format from `broadcast_message.h` + `custom_mining.h`:
/// ```text
/// [0..32]    sourcePublicKey (m256i) — dispatcher
/// [32..64]   zero / padding (m256i)
/// [64..96]   gammingNonce (m256i)
/// --- Inner CustomQubicMiningSolution ---
/// [96..128]  sourcePublicKey (miner, 32 bytes)
/// [128..136] jobId (u64 LE)
/// [136]      customMiningType (u8)
/// [137..]    solution-specific payload
/// [end-64..] sender's signature
/// ```
pub fn decode_custom_mining_solution(payload: &[u8]) -> Result<CustomMiningSolution> {
    // Minimum: 96 (header) + 32 (miner key) + 8 (jobId) + 1 (type) + 64 (signature) = 201
    if payload.len() < 201 {
        anyhow::bail!(
            "CustomMiningSolution payload too small: {} < 201",
            payload.len()
        );
    }

    let source_public_key_hex = hex::encode(&payload[0..32]); // dispatcher
    // bytes 32..64 are zero/padding, skip
    // bytes 64..96 are gammingNonce, skip

    let miner_public_key_hex = hex::encode(&payload[96..128]);
    let job_id = u64::from_le_bytes([
        payload[128], payload[129], payload[130], payload[131],
        payload[132], payload[133], payload[134], payload[135],
    ]);
    let custom_mining_type = payload[136];

    // Solution-specific payload: from offset 137 to end - 64 (signature)
    let sig_start = payload.len() - 64;
    let payload_hex = hex::encode(&payload[137..sig_start]);
    let signature_hex = hex::encode(&payload[sig_start..]);

    Ok(CustomMiningSolution {
        source_public_key_hex,
        miner_public_key_hex,
        job_id,
        custom_mining_type,
        payload_hex,
        signature_hex,
    })
}

/// Decode RespondPruningLog payload (type 57).
///
/// Layout: 8 bytes, `success` as i64 LE (0 = success, non-zero = error code).
pub fn decode_pruning_log_response(payload: &[u8]) -> Result<PruningLogResponse> {
    if payload.len() < 8 {
        anyhow::bail!(
            "PruningLogResponse payload too small: {} < 8",
            payload.len()
        );
    }

    let success = i64::from_le_bytes([
        payload[0], payload[1], payload[2], payload[3],
        payload[4], payload[5], payload[6], payload[7],
    ]);

    Ok(PruningLogResponse { success })
}

/// Decode RespondLogStateDigest payload (type 59).
///
/// Layout: 32 bytes, a single m256i digest.
pub fn decode_log_state_digest(payload: &[u8]) -> Result<LogStateDigest> {
    if payload.len() < 32 {
        anyhow::bail!(
            "LogStateDigest payload too small: {} < 32",
            payload.len()
        );
    }

    let digest_hex = hex::encode(&payload[..32]);

    Ok(LogStateDigest { digest_hex })
}

/// Decode RespondTxStatus payload (type 202, tx addon only).
///
/// Layout from LFG-Qubic C++ `RespondTxStatus` (`#pragma pack(push, 1)`):
/// ```text
/// [0..4]     currentTickOfNode (u32)
/// [4..8]     tick (u32)
/// [8..12]    txCount (u32)
/// [12..524]  moneyFlew bitfield ((4096+7)/8 = 512 bytes)
/// [524..]    txDigests[txCount] (32 bytes each)
/// ```
pub fn decode_tx_status(payload: &[u8], _tick: u32) -> Result<TxStatusResponse> {
    // Minimum: 12 (header) + 512 (moneyFlew) = 524
    if payload.len() < 524 {
        anyhow::bail!(
            "TxStatus payload too small: {} < 524",
            payload.len()
        );
    }

    let current_tick_of_node = u32::from_le_bytes(payload[0..4].try_into().unwrap());
    let resp_tick = u32::from_le_bytes(payload[4..8].try_into().unwrap());
    let tx_count = u32::from_le_bytes(payload[8..12].try_into().unwrap());
    let money_flew_hex = hex::encode(&payload[12..524]);

    // Only read txCount digests (rest of the buffer may not be sent)
    let digests_start = 524;
    let digests_end = digests_start + (tx_count as usize) * 32;
    let mut tx_digests = Vec::with_capacity(tx_count as usize);
    if digests_end <= payload.len() {
        for i in 0..tx_count as usize {
            let off = digests_start + i * 32;
            tx_digests.push(hex::encode(&payload[off..off + 32]));
        }
    }

    Ok(TxStatusResponse {
        current_tick_of_node,
        tick: resp_tick,
        tx_count,
        money_flew_hex,
        tx_digests,
    })
}

/// Decode RespondAllLogIdRangesFromTick payload (type 51).
///
/// Layout from C++ `logging.h`:
/// ```text
/// fromLogId[LOG_TX_PER_TICK] — i64 array (4102 * 8 bytes = 32816 bytes)
/// length[LOG_TX_PER_TICK]    — i64 array (4102 * 8 bytes = 32816 bytes)
/// ```
/// Total: 65632 bytes.
///
/// We decode all LOG_TX_PER_TICK entries.
pub fn decode_all_log_id_ranges(payload: &[u8], tick: u32) -> Result<LogIdRangesResponse> {
    let expected_size = LOG_TX_PER_TICK * 8 * 2; // fromLogId + length arrays
    if payload.len() < expected_size {
        anyhow::bail!(
            "LogIdRanges payload too small: {} < {}",
            payload.len(),
            expected_size
        );
    }

    let mut from_log_ids = Vec::with_capacity(LOG_TX_PER_TICK);
    let mut lengths = Vec::with_capacity(LOG_TX_PER_TICK);

    for i in 0..LOG_TX_PER_TICK {
        let off = i * 8;
        from_log_ids.push(i64::from_le_bytes(payload[off..off + 8].try_into().unwrap()));
    }

    let lengths_offset = LOG_TX_PER_TICK * 8;
    for i in 0..LOG_TX_PER_TICK {
        let off = lengths_offset + i * 8;
        lengths.push(i64::from_le_bytes(payload[off..off + 8].try_into().unwrap()));
    }

    Ok(LogIdRangesResponse {
        tick,
        tx_count: LOG_TX_PER_TICK,
        from_log_ids,
        lengths,
    })
}
