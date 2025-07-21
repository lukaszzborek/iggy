use std::{sync::Arc, task::{Context, Waker}};

use iggy_common::{DiagnosticEvent, IggyError};
use tokio::{io::AsyncWrite, sync::{mpsc, oneshot}};

use crate::proto::{connection::Connection, runtime::{oneshot, Runtime}};

pub struct State {
    pub(crate) inner: Connection,
    driver: Option<Waker>,
    on_connected: Option<oneshot::Sender<bool>>,
    connected: bool,
    events: mpsc::UnboundedSender<DiagnosticEvent>,
    pub(crate) blocked_writer: Option<Waker>,
    pub(crate) blocked_reader: Option<Waker>,
    pub(crate) error: Option<IggyError>,
    runtime: Arc<dyn Runtime>,
    send_buffer: Vec<u8>,
    socket: Box<dyn AsyncWrite>
}

impl State {
    fn drive_transmit(&mut self, cx: &mut Context) {
        let (code, payload) = self.inner.send_buf.pop_front().unwrap();
        
    }
}
