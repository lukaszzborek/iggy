pub mod connection;

use std::collections::VecDeque;

use bytes::Bytes;
use iggy_common::ClientState;

pub struct IggyCore {
    buffer: VecDeque<Bytes>,
    current_state: ClientState,
}

impl IggyCore {
    pub fn write(&mut self, payload: Bytes) {
        self.buffer.push_back(payload)
    }

}

