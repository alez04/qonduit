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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_header_size() {
        assert_eq!(std::mem::size_of::<RequestResponseHeader>(), 8);
    }

    #[test]
    fn test_new_request() {
        let header = RequestResponseHeader::new_request(27, 0, 0xDEADBEEF);
        assert_eq!(header.msg_type(), 27);
        assert_eq!(header.payload_size(), 0);
        assert_eq!(header.size(), 8); // header only
        assert_eq!(header.dejavu(), 0xDEADBEEF);
    }

    #[test]
    fn test_new_request_with_payload() {
        let header = RequestResponseHeader::new_request(31, 32, 42);
        assert_eq!(header.msg_type(), 31);
        assert_eq!(header.payload_size(), 32);
        assert_eq!(header.size(), 40); // 8 + 32
        assert_eq!(header.dejavu(), 42);
    }

    #[test]
    fn test_end_response() {
        let header = RequestResponseHeader::end_response(999);
        assert!(header.is_end_response());
        assert_eq!(header.msg_type(), 35);
        assert_eq!(header.dejavu(), 999);
    }

    #[test]
    fn test_header_byte_layout() {
        // type=27, payload=0, dejavu=1
        let header = RequestResponseHeader::new_request(27, 0, 1);
        let bytes: [u8; 8] = unsafe { std::mem::transmute(header) };
        // size = 8 (header only) -> LE: [0x08, 0x00, 0x00]
        assert_eq!(bytes[0], 0x08);
        assert_eq!(bytes[1], 0x00);
        assert_eq!(bytes[2], 0x00);
        // type
        assert_eq!(bytes[3], 27);
        // dejavu = 1 -> LE: [0x01, 0x00, 0x00, 0x00]
        assert_eq!(bytes[4], 0x01);
        assert_eq!(bytes[5], 0x00);
        assert_eq!(bytes[6], 0x00);
        assert_eq!(bytes[7], 0x00);
    }

    #[test]
    fn test_roundtrip() {
        let original = RequestResponseHeader::new_request(42, 1024, 0x12345678);
        let bytes: [u8; 8] = unsafe { std::mem::transmute(original) };
        let restored: &RequestResponseHeader =
            unsafe { &*(&bytes as *const [u8; 8] as *const RequestResponseHeader) };
        assert_eq!(restored.msg_type(), 42);
        assert_eq!(restored.payload_size(), 1024);
        assert_eq!(restored.dejavu(), 0x12345678);
    }
}
