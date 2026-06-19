/// 8-byte packet header that precedes every TCP message.
///
/// Layout (little-endian):
///   [0..3]  _size   — u24, total packet size including header
///   [3]     _type   — u8, message type
///   [4..8]  _dejavu — u32, request correlation ID
///
/// `dejavu` is randomized for requests and echoed back in responses.
/// `0xFFFFFFFF` signals end-of-response.
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct RequestResponseHeader {
    _size: [u8; 3],
    _type: u8,
    _dejavu: u32,
}

const _: () = assert!(std::mem::size_of::<RequestResponseHeader>() == 8);

impl RequestResponseHeader {
    /// Total packet size including header.
    pub fn size(&self) -> u32 {
        let raw = u32::from_le_bytes([self._size[0], self._size[1], self._size[2], 0]);
        if raw == 0 {
            return u32::MAX; // broken packet
        }
        raw
    }

    /// Message type.
    pub fn msg_type(&self) -> u8 {
        self._type
    }

    /// Parsed message type.
    pub fn network_type(&self) -> Option<super::NetworkMessageType> {
        super::NetworkMessageType::from_u8(self._type)
    }

    /// Dejavu correlation ID.
    pub fn dejavu(&self) -> u32 {
        u32::from_le(self._dejavu)
    }

    /// Payload size (total size minus header).
    pub fn payload_size(&self) -> u32 {
        self.size().saturating_sub(8)
    }

    /// True if this is an end-response marker.
    pub fn is_end_response(&self) -> bool {
        self._type == 35 // END_RESPONSE
    }

    // --- Builders ---

    /// Create a new header for a request.
    pub fn new_request(msg_type: u8, payload_size: u32, dejavu: u32) -> Self {
        let total = payload_size + 8;
        let s = total.to_le_bytes();
        Self {
            _size: [s[0], s[1], s[2]],
            _type: msg_type,
            _dejavu: dejavu.to_le(),
        }
    }

    /// Create a response header echoing the request's dejavu.
    pub fn new_response(msg_type: u8, payload_size: u32, dejavu: u32) -> Self {
        Self::new_request(msg_type, payload_size, dejavu)
    }

    /// Create an end-response header.
    pub fn end_response(dejavu: u32) -> Self {
        Self::new_response(35, 0, dejavu)
    }
}
