use std::collections::VecDeque;

use bytes::Bytes;
use iggy_common::{ClientState, IggyDuration, IggyError, IggyTimestamp};
use tracing::{info, trace};

pub struct SendBuffer {
    data: VecDeque<Bytes>
}

impl SendBuffer {
    pub fn write(&mut self, payload: Bytes) {
        self.data.push_back(payload);
    }

    pub fn poll_transmit(&mut self) -> Option<Bytes> {
        self.data.pop_front()
    }
}

pub struct IggyCore {
    data: VecDeque<Bytes>,
    state: ClientState,
    connected_at: Option<IggyTimestamp>,
    reconnect_interval: u64, // micros
}

impl IggyCore {
    pub fn write(&mut self, payload: Bytes) {
        self.data.push_back(payload);
    }

    /*
    
    // Псевдо-код адаптера:
    loop {
        if let Some(wait) = core.reconnect_wait() {
            // тут — await, если надо (или thread::sleep)
            sleep(wait.get_duration()).await;
        }
        match core.connect() {
            Ok(_) => break,
            Err(e) => {
                // обработать ошибку
                break;
            }
        }
    }
 */
    pub fn connect(&mut self) -> Result<(), IggyError> {
        match self.state {
            ClientState::Shutdown => {
                trace!("Cannot connect. Client is shutdown.");
                return Err(IggyError::ClientShutdown);
            }
            ClientState::Connected | ClientState::Authenticating | ClientState::Authenticated => {
                trace!("Client is already connected.");
                return Ok(());
            }
            ClientState::Connecting => {
                trace!("Client is already connecting.");
                return Ok(());
            }
            _ => {}
        };

        self.state = ClientState::Connecting;
        Ok(())
    }

    pub fn reconnect_wait(&self) -> Option<IggyDuration> {
        if let Some(connected_at) = self.connected_at.as_ref() {
            let now = IggyTimestamp::now();
            let elapsed = now.as_micros() - connected_at.as_micros();
            if elapsed < self.reconnect_interval {
                Some(IggyDuration::from(self.reconnect_interval - elapsed))
            } else {
                None
            }
        } else {
            None
        }
    }
}

