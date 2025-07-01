use bytes::Bytes;
use iggy_common::IggyError;

use crate::proto::{connection::IggyCore, runtime::{Lockable, Runtime}};

pub struct TCPAdapter<R: Runtime> {
    rt: R,
    core: R::Mutex<IggyCore>,
}

impl<R: Runtime> TCPAdapter<R> {
    async fn send_raw(&self, code: u32, payload: Bytes) -> Result<Bytes, IggyError> {
        let a = self.rt.mutex(payload);
        let _ = a.lock().await;
        
        Ok(Bytes::new())
    }
}
