use std::sync::Arc;

use crate::proto::{connection::IggyCore, runtime::{sync, Runtime}};

pub trait Driver {
    fn start(&self);
}

pub struct QuicDriver<R>
where
    R: Runtime
{
    core: sync::Mutex<IggyCore>,
    rt: Arc<R>,
    notify: Arc<sync::Notify>,
}

impl<R> Driver for QuicDriver<R>
where
    R: Runtime
{
    fn start(&self) {
        let rt = self.rt.clone();
        let nt = self.notify.clone();
        rt.spawn(Box::pin(async {
            loop {
                nt.notified().await
            }
        }));
    }
}
