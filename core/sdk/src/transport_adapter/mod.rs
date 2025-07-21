pub mod r#async;
pub mod async_new;
pub mod connection;
pub mod state;

use std::{pin::Pin, sync::Arc};

use bytes::Bytes;
use futures::FutureExt;
use iggy_common::{Command, IggyError};

use crate::proto::runtime::sync::OneShotReceiver;

pub struct RespFut {
    rx: OneShotReceiver<Bytes>
}

impl Future for RespFut {
    type Output = Result<Bytes, IggyError>;

    fn poll(self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<Self::Output> {
        self.get_mut().rx.poll_unpin(cx).map_err(|_| IggyError::ReceiveError)
    }
}
