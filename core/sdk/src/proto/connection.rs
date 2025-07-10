use std::{collections::VecDeque, io::Cursor, pin::Pin, sync::Arc};

use bytes::{Buf, BufMut, Bytes, BytesMut};
use iggy_common::{ClientState, Command, IggyDuration, IggyError, IggyErrorDiscriminants, IggyTimestamp};
use std::io::IoSlice;
use tracing::{error, trace};

const REQUEST_INITIAL_BYTES_LENGTH: usize = 4;
const RESPONSE_INITIAL_BYTES_LENGTH: usize = 8;
const ALREADY_EXISTS_STATUSES: &[u32] = &[
    IggyErrorDiscriminants::TopicIdAlreadyExists as u32,
    IggyErrorDiscriminants::TopicNameAlreadyExists as u32,
    IggyErrorDiscriminants::StreamIdAlreadyExists as u32,
    IggyErrorDiscriminants::StreamNameAlreadyExists as u32,
    IggyErrorDiscriminants::UserAlreadyExists as u32,
    IggyErrorDiscriminants::PersonalAccessTokenAlreadyExists as u32,
    IggyErrorDiscriminants::ConsumerGroupIdAlreadyExists as u32,
    IggyErrorDiscriminants::ConsumerGroupNameAlreadyExists as u32,
];

pub trait TransportConfig {
    fn resstablish_after(&self) -> IggyDuration;
    fn max_retries(&self) -> Option<u32>;
}

pub struct TxBuf {
    pub id: u64,
    hdr_len: [u8; 4],
    hdr_code: [u8; 4],
    payload: Bytes,
}

impl TxBuf {
    pub fn as_slices(&self) -> [IoSlice<'_>; 3] {
        [
            IoSlice::new(&self.hdr_len),
            IoSlice::new(&self.hdr_code),
            IoSlice::new(&self.payload),
        ]
    }
}

#[derive(Debug)]
pub enum Order {
    Outbound(Box<dyn Command>),
    State(ClientState),
    Wait(IggyDuration),
    Response(Bytes),
    Reconnect,
    InitialConnect,
    Noop,
}

pub enum InboundResult {
    Need(usize),        
    Ready(usize),    
    Error(IggyError),   
}

pub struct IggyCore {
    state: ClientState,
    last_connect: Option<IggyTimestamp>,
    pending: VecDeque<(u32 /* code */, Bytes /* payload */, u64 /* transport_id */)>,
    config: Arc<dyn TransportConfig + Send + Sync + 'static>, // todo rewrite via generic
    retry_count: u32,
    current_tx: Option<Arc<TxBuf>>,
}

impl IggyCore {
    pub fn write(&mut self, cmd: &impl Command, id: u64) -> Result<(), IggyError> {
        match self.state {
            ClientState::Shutdown => {
                trace!("Cannot send data. Client is shutdown.");
                return Err(IggyError::ClientShutdown);
            }
            ClientState::Disconnected => {
                trace!("Cannot send data. Client is not connected.");
                return Err(IggyError::NotConnected);
            }
            ClientState::Connecting => {
                trace!("Cannot send data. Client is still connecting.");
                return Err(IggyError::NotConnected);
            }
            _ => {}
        }
        self.pending.push_back((cmd.code(), cmd.to_bytes(), id));
        Ok(())
    }

    pub fn start_connect(&mut self) -> Result<Order, IggyError> {
        self.connect(Order::InitialConnect)
    }

    pub fn poll_connect(&mut self) -> Result<Order, IggyError> {
        self.connect(Order::Reconnect)
    }

    fn connect(&mut self, request: Order) -> Result<Order, IggyError> {
        match (self.state, request) {
            (ClientState::Shutdown, _) => {
                trace!("Cannot connect. Client is shutdown.");
                return Err(IggyError::ClientShutdown);
            }
            (
                ClientState::Connected | ClientState::Authenticating | ClientState::Authenticated,
                _,
            ) => {
                trace!("Client: client_address is already connected.");
                return Ok(Order::Noop);
            }
            (ClientState::Connecting, Order::Reconnect) => {}
            (ClientState::Connecting, _) => {
                trace!("Client is already connecting.");
                return Ok(Order::Noop);
            }
            _ => {}
        };

        self.state = ClientState::Connecting;

        if let Some(max_retries) = self.config.max_retries() {
            if self.retry_count >= max_retries {
                self.state = ClientState::Disconnected;
                return Err(IggyError::CannotEstablishConnection);
            }
        }

        if let Some(last_connect) = self.last_connect {
            let now = IggyTimestamp::now();
            let elapsed = now.as_micros() - last_connect.as_micros();
            let interval = self.config.resstablish_after().as_micros();
            if elapsed < interval {
                let remaining = IggyDuration::from(interval - elapsed);
                return Ok(Order::Wait(remaining));
            }
        }

        self.retry_count += 1;
        self.last_connect = Some(IggyTimestamp::now());
        Ok(Order::Reconnect)
    }

    pub fn poll_transmit(&mut self) -> Option<Arc<TxBuf>> {
        if self.current_tx.is_none() {
            let (code, payload, id) = self.pending.pop_front()?;
            let len = (payload.len() + REQUEST_INITIAL_BYTES_LENGTH) as u32;

            self.current_tx = Some(Arc::new(TxBuf{
                hdr_len: len.to_le_bytes(),
                hdr_code: code.to_le_bytes(),
                payload, 
                id,
            }));
        }
        self.current_tx.as_ref().cloned()
    }

    pub fn mark_tx_done(&mut self) {
        self.current_tx = None
    }

    pub fn feed_inbound(&mut self, mut cur: Cursor<&[u8]>) -> InboundResult {
        let buf_len = cur.get_ref().len();
        if buf_len < RESPONSE_INITIAL_BYTES_LENGTH {
            return InboundResult::Need(RESPONSE_INITIAL_BYTES_LENGTH - buf_len);
        }

        let status  = cur.get_u32_le();
        let length  = cur.get_u32_le();

        if status != 0 {
            if ALREADY_EXISTS_STATUSES.contains(&status) {
                tracing::debug!(
                    "Received a server resource already exists response: {} ({})",
                    status,
                    IggyError::from_code_as_string(status)
                )
            } else {
                error!(
                    "Received an invalid response with status: {} ({}).",
                    status,
                    IggyError::from_code_as_string(status),
                );
            }
            return InboundResult::Error(IggyError::from_code(status));
        }

        trace!("Status: OK. Response length: {}", length);
        if length <= 1 {
            return InboundResult::Ready(0);
        }

        let total = RESPONSE_INITIAL_BYTES_LENGTH + length as usize;
        if buf_len < total {
            return InboundResult::Need(total - buf_len);
        }

        InboundResult::Ready(total)
    }

    pub fn on_transport_connected(&mut self) {
        self.state        = ClientState::Connected;
        self.retry_count  = 0;
        self.last_connect = Some(IggyTimestamp::now());
    }

    pub fn on_transport_disconnected(&mut self) {
        self.state = ClientState::Disconnected;
    }
}
