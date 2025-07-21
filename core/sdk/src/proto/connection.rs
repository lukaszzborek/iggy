use std::{collections::VecDeque, io::{self, Cursor}, net::SocketAddr, pin::Pin, str::FromStr, sync::Arc, task::{Context, Waker}};

use bytes::{Buf, BufMut, Bytes, BytesMut};
use futures::AsyncWrite;
use iggy_common::{ClientState, Command, DiagnosticEvent, IggyDuration, IggyError, IggyErrorDiscriminants, IggyTimestamp};
use tokio::sync::{mpsc, oneshot};
use tokio_util::io::poll_write_buf;
use std::io::IoSlice;
use tracing::{error, trace};

use crate::proto::runtime::Runtime;

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

pub struct Connection {
    server_address: SocketAddr,
    state: ClientState,
    config: IggyCoreConfig,

    pub(crate) send_buf: VecDeque<(u32 /* code */, Bytes /* payload */)>,
    send_waker: Option<Waker>,

    pub recv_buf: VecDeque<Bytes>,
    recv_waker: Option<Waker>,
}

impl Connection {
    pub fn new(config: IggyCoreConfig, server_address: SocketAddr) -> Self {
        Self {
            server_address,
            state: ClientState::Disconnected,
            config,
            send_buf: VecDeque::new(),
            send_waker: None,
            recv_buf: VecDeque::new(),
            recv_waker: None,
        }
    }

    // TODO add iggy code
    pub fn write(&mut self, code: u32, data: Bytes) -> Result<(), IggyError> {
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

        self.send_buf.push_back((code, data));

        Ok(())
    }

    pub fn poll_transmit(&mut self) -> Option<TxBuf> {
        let (code, payload) = self.send_buf.pop_front()?;
        let len = (payload.len() + REQUEST_INITIAL_BYTES_LENGTH) as u32;
        Some(TxBuf{
            hdr_len: len.to_le_bytes(),
            hdr_code: code.to_le_bytes(),
            payload,
            id: 1, // todo rm
        })
    }

    pub fn is_drained(&self) -> bool {
        matches!(self.state, ClientState::Shutdown | ClientState::Disconnected)
    }

    // pub fn poll_transmit(&mut self, buf: &mut Vec<u8>) -> Result<(), IggyError> {
    //     match self.state {
    //         ClientState::Shutdown => {
    //             trace!("Cannot send data. Client is shutdown.");
    //             return Err(IggyError::ClientShutdown);
    //         }
    //         ClientState::Disconnected => {
    //             trace!("Cannot send data. Client is not connected.");
    //             return Err(IggyError::NotConnected);
    //         }
    //         ClientState::Connecting => {
    //             trace!("Cannot send data. Client is still connecting.");
    //             return Err(IggyError::NotConnected);
    //         }
    //         _ => {}
    //     }

    //     let (code, payload, id) = self.pending.pop_front()?;
    //     let len = (payload.len() + REQUEST_INITIAL_BYTES_LENGTH) as u32;

    //     self.current_tx = Some(Arc::new(TxBuf{
    //         hdr_len: len.to_le_bytes(),
    //         hdr_code: code.to_le_bytes(),
    //         payload, 
    //         id,
    //     }));

    // }
}

#[derive(Debug)]
pub struct IggyCoreConfig {
    max_retries: Option<u32>,
    reestablish_after: IggyDuration,
}

impl Default for IggyCoreConfig {
    fn default() -> Self {
        Self { max_retries: None, reestablish_after: IggyDuration::from_str("5s").unwrap() }
    }
}

//////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
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
    Ready(usize, usize),    
    Error(IggyError),   
}

#[derive(Debug)]
pub struct IggyCore {
    pub(crate) state: ClientState,
    last_connect: Option<IggyTimestamp>,
    pending: VecDeque<(u32 /* code */, Bytes /* payload */, u64 /* transport_id */)>,
    config: IggyCoreConfig,
    retry_count: u32,
    current_tx: Option<Arc<TxBuf>>,
}

impl IggyCore {
    pub fn new(config: IggyCoreConfig) -> Self {
        Self {
            state: ClientState::Disconnected,
            last_connect: None,
            pending: VecDeque::new(),
            config,
            retry_count: 0,
            current_tx: None,
        }
    }

    pub fn write(&mut self, code: u32, payload: Bytes, id: u64) -> Result<(), IggyError> {
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
        self.pending.push_back((code, payload, id));
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

        if let Some(max_retries) = self.config.max_retries {
            if self.retry_count >= max_retries {
                self.state = ClientState::Disconnected;
                return Err(IggyError::CannotEstablishConnection);
            }
        }

        if let Some(last_connect) = self.last_connect {
            let now = IggyTimestamp::now();
            let elapsed = now.as_micros() - last_connect.as_micros();
            let interval = self.config.reestablish_after.as_micros();
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

    pub fn initial_bytes_len(&self) -> usize {
        RESPONSE_INITIAL_BYTES_LENGTH
    }

    pub fn feed_inbound(&self, cur: &[u8]) -> InboundResult {
        let buf_len = cur.len();
        if buf_len < RESPONSE_INITIAL_BYTES_LENGTH {
            return InboundResult::Need(RESPONSE_INITIAL_BYTES_LENGTH - buf_len);
        }

        let status = match cur[..4].try_into() {
            Ok(bytes) => u32::from_le_bytes(bytes),
            Err(_) => return InboundResult::Error(IggyError::InvalidNumberEncoding),
        };

        let length = match cur[4..8].try_into() {
            Ok(bytes) => u32::from_le_bytes(bytes),
            Err(_) => return InboundResult::Error(IggyError::InvalidNumberEncoding),
        };

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
            return InboundResult::Ready(0, 0);
        }

        let total = RESPONSE_INITIAL_BYTES_LENGTH + length as usize;
        if buf_len < total {
            return InboundResult::Need(total - buf_len);
        }

        InboundResult::Ready(8, total)
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

pub fn feed_inbound(cur: &[u8]) -> InboundResult {
    let buf_len = cur.len();
    if buf_len < RESPONSE_INITIAL_BYTES_LENGTH {
        return InboundResult::Need(RESPONSE_INITIAL_BYTES_LENGTH - buf_len);
    }

    let status = match cur[..4].try_into() {
        Ok(bytes) => u32::from_le_bytes(bytes),
        Err(_) => return InboundResult::Error(IggyError::InvalidNumberEncoding),
    };

    let length = match cur[4..8].try_into() {
        Ok(bytes) => u32::from_le_bytes(bytes),
        Err(_) => return InboundResult::Error(IggyError::InvalidNumberEncoding),
    };

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
        return InboundResult::Ready(0, 0);
    }

    let total = RESPONSE_INITIAL_BYTES_LENGTH + length as usize;
    if buf_len < total {
        return InboundResult::Need(total - buf_len);
    }

    InboundResult::Ready(8, total)
}

