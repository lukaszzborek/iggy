use std::{collections::VecDeque, pin::Pin, sync::Arc};

use bytes::Bytes;
use iggy_common::{ClientState, Command, IggyDuration, IggyError, IggyTimestamp};
use std::io::IoSlice;
use tracing::trace;

const REQUEST_INITIAL_BYTES_LENGTH: usize = 4;

pub trait TransportConfig {
    fn resstablish_after(&self) -> IggyDuration;
    fn max_retries(&self) -> Option<u32>;
}

pub struct TxBuf {
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

pub enum Order {
    Outbound(Box<dyn Command>),
    State(ClientState),
    Wait(IggyDuration),
    Response(Bytes),
    Reconnect,
    InitialConnect,
    Noop,
}

pub struct IggyCore {
    state: ClientState,
    last_connect: Option<IggyTimestamp>,
    reconnect_us: u64,
    pending: VecDeque<Box<dyn Command>>,
    config: Box<dyn TransportConfig>, // todo rewrite via generic
    retry_count: u32,
    current_tx: Option<TxBuf>,
}

impl IggyCore {
    pub fn write(&mut self, cmd: impl Command) -> Result<_, IggyError> {
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
        self.pending.push_back(Box::new(cmd));
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

    // TODO вызывать при async fn poll
    pub fn poll_transmit(&mut self) -> Option<&TxBuf> {
        if self.current_tx.is_none() {
            let cmd = self.pending.pop_front()?;
            let payload = cmd.to_bytes();
            let len = (payload.len() + REQUEST_INITIAL_BYTES_LENGTH) as u32;

            self.current_tx = Some(TxBuf {
                hdr_len: len.to_le_bytes(),
                hdr_code: cmd.code().to_le_bytes(),
                payload,
            });
        }
        self.current_tx.as_ref()
    }
}

// pub trait ConnectionAdapter: Send + Sync + 'static {
//     fn write(&mut self, buf: &[u8]) -> Pin<Box<dyn Future<Output = Result<(), IggyError>> + Send>>;
// }

// pub trait Runtime {

// }

// // pin_project! {
// pub struct OpenConn<'a> {
//     conn: &'a Connection,
// }
// // }

// impl Future for OpenConn<'_> {
//     type Output = Result<Connection, IggyError>;
//     fn poll(self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<Self::Output> {

//     }
// }

// pub struct Connection {
//     adapter: Arc<dyn ConnectionAdapter>,
//     runtime: Arc<dyn Runtime>,
// }

// impl Connection {

// }
