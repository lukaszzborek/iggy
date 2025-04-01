use bytes::Bytes;
use iggy::prelude::IggyMessage;
use std::time::Duration;

pub fn put_timestamp_in_first_message(message: &mut IggyMessage) {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_micros() as u64;

    let mut payload_vec = message.payload.to_vec();
    if payload_vec.len() < 8 {
        let mut new_payload = vec![0u8; 8];
        new_payload.extend_from_slice(&payload_vec);
        payload_vec = new_payload;
    }
    let timestamp_bytes = now.to_le_bytes();
    payload_vec[0..8].copy_from_slice(&timestamp_bytes);
    message.payload = Bytes::from(payload_vec);
}

pub fn calculate_latency_from_first_message(message: &IggyMessage) -> Duration {
    let send_timestamp = u64::from_le_bytes(message.payload[0..8].try_into().unwrap());
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_micros() as u64;
    Duration::from_micros(now - send_timestamp)
}
