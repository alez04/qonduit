/// Network message types from `network_message_type.h`.
///
/// These map directly to the `type` field in the 8-byte packet header.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NetworkMessageType {
    ExchangePublicPeers = 0,
    BroadcastMessage = 1,
    BroadcastComputors = 2,
    BroadcastTick = 3,
    BroadcastFutureTickData = 8,
    RequestComputors = 11,
    RequestQuorumTick = 14,
    RequestTickData = 16,
    BroadcastTransaction = 24,
    RequestTransactionInfo = 26,
    RequestCurrentTickInfo = 27,
    RespondCurrentTickInfo = 28,
    RequestTickTransactions = 29,
    RequestEntity = 31,
    RespondEntity = 32,
    RequestContractIpo = 33,
    RespondContractIpo = 34,
    EndResponse = 35,
    RequestIssuedAssets = 36,
    RespondIssuedAssets = 37,
    RequestOwnedAssets = 38,
    RespondOwnedAssets = 39,
    RequestPossessedAssets = 40,
    RespondPossessedAssets = 41,
    RequestContractFunction = 42,
    RespondContractFunction = 43,
    RequestLog = 44,
    RespondLog = 45,
    RequestSystemInfo = 46,
    RespondSystemInfo = 47,
    RequestLogIdRangeFromTx = 48,
    RespondLogIdRangeFromTx = 49,
    RequestAllLogIdRangesFromTx = 50,
    RespondAllLogIdRangesFromTx = 51,
    RequestAssets = 52,
    RespondAssets = 53,
    TryAgain = 54,
    RequestPruningLog = 56,
    RespondPruningLog = 57,
    RequestLogStateDigest = 58,
    RespondLogStateDigest = 59,
    RequestActiveIpos = 64,
    RespondActiveIpo = 65,
    RequestOracleData = 66,
    RespondOracleData = 67,
    BroadcastCustomMiningTask = 68,
    BroadcastCustomMiningSolution = 69,
    SpecialCommand = 255,
}

impl NetworkMessageType {
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(Self::ExchangePublicPeers),
            1 => Some(Self::BroadcastMessage),
            2 => Some(Self::BroadcastComputors),
            3 => Some(Self::BroadcastTick),
            8 => Some(Self::BroadcastFutureTickData),
            11 => Some(Self::RequestComputors),
            14 => Some(Self::RequestQuorumTick),
            16 => Some(Self::RequestTickData),
            24 => Some(Self::BroadcastTransaction),
            26 => Some(Self::RequestTransactionInfo),
            27 => Some(Self::RequestCurrentTickInfo),
            28 => Some(Self::RespondCurrentTickInfo),
            29 => Some(Self::RequestTickTransactions),
            31 => Some(Self::RequestEntity),
            32 => Some(Self::RespondEntity),
            33 => Some(Self::RequestContractIpo),
            34 => Some(Self::RespondContractIpo),
            35 => Some(Self::EndResponse),
            36 => Some(Self::RequestIssuedAssets),
            37 => Some(Self::RespondIssuedAssets),
            38 => Some(Self::RequestOwnedAssets),
            39 => Some(Self::RespondOwnedAssets),
            40 => Some(Self::RequestPossessedAssets),
            41 => Some(Self::RespondPossessedAssets),
            42 => Some(Self::RequestContractFunction),
            43 => Some(Self::RespondContractFunction),
            44 => Some(Self::RequestLog),
            45 => Some(Self::RespondLog),
            46 => Some(Self::RequestSystemInfo),
            47 => Some(Self::RespondSystemInfo),
            48 => Some(Self::RequestLogIdRangeFromTx),
            49 => Some(Self::RespondLogIdRangeFromTx),
            50 => Some(Self::RequestAllLogIdRangesFromTx),
            51 => Some(Self::RespondAllLogIdRangesFromTx),
            52 => Some(Self::RequestAssets),
            53 => Some(Self::RespondAssets),
            54 => Some(Self::TryAgain),
            56 => Some(Self::RequestPruningLog),
            57 => Some(Self::RespondPruningLog),
            58 => Some(Self::RequestLogStateDigest),
            59 => Some(Self::RespondLogStateDigest),
            64 => Some(Self::RequestActiveIpos),
            65 => Some(Self::RespondActiveIpo),
            66 => Some(Self::RequestOracleData),
            67 => Some(Self::RespondOracleData),
            68 => Some(Self::BroadcastCustomMiningTask),
            69 => Some(Self::BroadcastCustomMiningSolution),
            255 => Some(Self::SpecialCommand),
            _ => None,
        }
    }

    /// True if this type is a request (client → node).
    pub fn is_request(&self) -> bool {
        matches!(
            self,
            Self::RequestComputors
                | Self::RequestQuorumTick
                | Self::RequestTickData
                | Self::RequestTransactionInfo
                | Self::RequestCurrentTickInfo
                | Self::RequestTickTransactions
                | Self::RequestEntity
                | Self::RequestContractIpo
                | Self::RequestIssuedAssets
                | Self::RequestOwnedAssets
                | Self::RequestPossessedAssets
                | Self::RequestContractFunction
                | Self::RequestLog
                | Self::RequestSystemInfo
                | Self::RequestLogIdRangeFromTx
                | Self::RequestAllLogIdRangesFromTx
                | Self::RequestAssets
                | Self::RequestPruningLog
                | Self::RequestLogStateDigest
                | Self::RequestActiveIpos
                | Self::RequestOracleData
        )
    }

    /// True if this type is a response (node → client).
    pub fn is_response(&self) -> bool {
        matches!(
            self,
            Self::RespondCurrentTickInfo
                | Self::RespondEntity
                | Self::RespondContractIpo
                | Self::RespondIssuedAssets
                | Self::RespondOwnedAssets
                | Self::RespondPossessedAssets
                | Self::RespondContractFunction
                | Self::RespondLog
                | Self::RespondSystemInfo
                | Self::RespondLogIdRangeFromTx
                | Self::RespondAllLogIdRangesFromTx
                | Self::RespondAssets
                | Self::RespondPruningLog
                | Self::RespondLogStateDigest
                | Self::RespondActiveIpo
                | Self::RespondOracleData
                | Self::EndResponse
                | Self::TryAgain
        )
    }

    /// True if this is a broadcast (node → all clients).
    pub fn is_broadcast(&self) -> bool {
        matches!(
            self,
            Self::BroadcastMessage
                | Self::BroadcastComputors
                | Self::BroadcastTick
                | Self::BroadcastFutureTickData
                | Self::BroadcastTransaction
                | Self::BroadcastCustomMiningTask
                | Self::BroadcastCustomMiningSolution
        )
    }
}
