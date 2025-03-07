use bytes::{BufMut, Bytes, BytesMut};

use crate::{
    bytes_serializable::BytesSerializable,
    error::IggyError,
    utils::{byte_size::IggyByteSize, sizeable::Sizeable},
};

pub const IGGY_MESSAGE_HEADER_SIZE: u32 = 4 + 16 + 8 + 8 + 8 + 4 + 4 + 8;

#[derive(Debug, serde::Serialize, serde::Deserialize, PartialEq, Default)]
pub struct IggyMessageHeader {
    /// Total size of message, including headers and payload
    pub record_size: u32,

    /// Message ID
    pub id: u128,

    /// Message offset
    pub offset: u64,

    /// Timestamp of the message after reception
    pub timestamp: u64,

    /// Timestamp taken on client side, just before sending the message
    pub origin_timestamp: u64,

    /// Size of headers section, in bytes
    pub headers_length: u32,

    /// Size of payload section, in bytes
    pub payload_length: u32,

    /// Checksum
    pub checksum: u64,
}

impl Sizeable for IggyMessageHeader {
    fn get_size_bytes(&self) -> IggyByteSize {
        (IGGY_MESSAGE_HEADER_SIZE as u64).into()
    }
}

impl BytesSerializable for IggyMessageHeader {
    fn to_bytes(&self) -> Bytes {
        let mut bytes = BytesMut::with_capacity(self.get_size_bytes().as_bytes_usize());
        bytes.put_u32_le(self.record_size);
        bytes.put_u128_le(self.id);
        bytes.put_u64_le(self.offset);
        bytes.put_u64_le(self.timestamp);
        bytes.put_u64_le(self.origin_timestamp);
        bytes.put_u32_le(self.headers_length);
        bytes.put_u32_le(self.payload_length);
        bytes.put_u64_le(self.checksum);
        bytes.freeze()
    }

    fn from_bytes(bytes: Bytes) -> Result<Self, IggyError> {
        if bytes.len() != IGGY_MESSAGE_HEADER_SIZE as usize {
            return Err(IggyError::InvalidCommand);
        }

        // Read record_size (4 bytes): 0-4
        let record_size = u32::from_le_bytes(
            bytes[..4]
                .try_into()
                .map_err(|_| IggyError::InvalidNumberEncoding)?,
        );

        // Read id (16 bytes): 4-20
        let id = u128::from_le_bytes(
            bytes[4..20]
                .try_into()
                .map_err(|_| IggyError::InvalidNumberEncoding)?,
        );

        // Read offset (8 bytes): 20-28
        let offset = u64::from_le_bytes(
            bytes[20..28]
                .try_into()
                .map_err(|_| IggyError::InvalidNumberEncoding)?,
        );

        // Read timestamp (8 bytes): 28-36
        let timestamp = u64::from_le_bytes(
            bytes[28..36]
                .try_into()
                .map_err(|_| IggyError::InvalidNumberEncoding)?,
        );

        // Read origin_timestamp (8 bytes): 36-44
        let origin_timestamp = u64::from_le_bytes(
            bytes[36..44]
                .try_into()
                .map_err(|_| IggyError::InvalidNumberEncoding)?,
        );

        // Read headers_length (4 bytes): 44-48
        let headers_length = u32::from_le_bytes(
            bytes[44..48]
                .try_into()
                .map_err(|_| IggyError::InvalidNumberEncoding)?,
        );

        // Read payload_length (4 bytes): 48-52
        let payload_length = u32::from_le_bytes(
            bytes[48..52]
                .try_into()
                .map_err(|_| IggyError::InvalidNumberEncoding)?,
        );

        // Read checksum (8 bytes): 52-60
        let checksum = u64::from_le_bytes(
            bytes[52..60]
                .try_into()
                .map_err(|_| IggyError::InvalidNumberEncoding)?,
        );

        Ok(IggyMessageHeader {
            record_size,
            id,
            offset,
            timestamp,
            origin_timestamp,
            headers_length,
            payload_length,
            checksum,
        })
    }
}
