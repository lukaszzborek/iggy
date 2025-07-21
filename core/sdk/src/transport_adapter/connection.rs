use std::{io, ops::Deref, sync::{Arc, Mutex}, task::{Context, Poll, Waker}};
use crate::{
    proto::{connection::Connection, runtime::{Runtime}},
};

use iggy_common::{DiagnosticEvent, IggyError};
use tokio::{io::AsyncWrite, sync::{mpsc, oneshot}};

use crate::proto::{connection::{TxBuf}};

pub struct Connecting {
    conn: Connection,
    events: mpsc::UnboundedSender<DiagnosticEvent>,
}

impl Connecting {
    
}

pub struct State {
    pub(crate) inner: Connection,
    driver: Option<Waker>,
    on_connected: Option<oneshot::Sender<bool>>,
    connected: bool,
    // events: mpsc::UnboundedSender<DiagnosticEvent>,
    pub(crate) blocked_writer: Option<Waker>,
    pub(crate) blocked_reader: Option<Waker>,
    pub(crate) error: Option<IggyError>,
    runtime: Arc<dyn Runtime>,
    send_buffer: Vec<u8>,
    current_tx: Option<TxBuf>,
    // socket: Box<dyn AsyncWrite>
}

impl State {
    fn drive_transmit(&mut self, cx: &mut Context) -> io::Result<bool> {
        let tx_buf = match &self.current_tx {
            Some(tx) => tx,
            None => {
                let tx_buf = self.inner.poll_transmit();
                if tx_buf.is_none() {
                    return Ok(false)
                }
                self.current_tx = tx_buf;
                self.current_tx.as_ref().unwrap()
            }
        };

        // try_send to socket

        Ok(true)
    }
}

pub struct ConnectionInner {
    pub(crate) state: Mutex<State>,
}

pub struct ConnectionRef(Arc<ConnectionInner>);

impl Deref for ConnectionRef {
    type Target = ConnectionInner;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl ConnectionRef {
    fn new(conn: Connection, runtime: Arc<dyn Runtime>) -> Self {
        Self(Arc::new(ConnectionInner {
            state: Mutex::new(State{
                inner: conn,
                driver: None,
                on_connected: None,
                connected: false,
                blocked_writer: None,
                blocked_reader: None,
                error: None,
                runtime,
                send_buffer: Vec::new(),
                current_tx: None,
            })
        }))
    }
}

pub struct ConnectionDriver(ConnectionRef);

impl Future for ConnectionDriver {
    type Output = Result<(), io::Error>;

    fn poll(self: std::pin::Pin<&mut Self>, cx: &mut Context<'_>) -> std::task::Poll<Self::Output> {
        let mut conn = self.0.state.lock().unwrap();
        let _ = conn.drive_transmit(cx)?;

        if !conn.inner.is_drained() {
            conn.driver = Some(cx.waker().clone());
            return Poll::Pending;
        }
        Poll::Ready(Ok(()))
    }
}