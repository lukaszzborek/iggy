use std::{sync::Mutex, task::Context};

use bytes::Bytes;
use futures::future::poll_fn;
use iggy_common::{Command, IggyError};

use crate::proto::{connection::IggyCore, runtime::{Lockable, Runtime}};
use crate::tcp::tcp_connection_stream_kind::ConnectionStreamKind;

pub struct TCPAdapter<R: Runtime> {
    rt: R,
    core: Mutex<IggyCore>,
    pub(crate) stream: R::Mutex<Option<ConnectionStreamKind>>,
}

impl<R: Runtime> TCPAdapter<R> {
    pub async fn send_with_response(&self, command: &impl Command) -> Result<Bytes, IggyError> {
        // poll_fn(|cx| )
        Ok(Bytes::new())
    }

    fn write(&self, command: &impl Command) -> Result<(), IggyError> {
        command.validate()?;
        self.core.lock().unwrap().write(command)
    }

    fn execute_poll(&mut self, cx: &mut Context, command: &impl Command) -> Result<Bytes, IggyError> {
        self.write(command)?;
        
    }
}
