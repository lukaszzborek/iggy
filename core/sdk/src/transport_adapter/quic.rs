use std::{pin::Pin, sync::{Arc, Mutex}};

use bytes::Bytes;

use crate::{connection::quic::QuicFactory, proto::{connection::IggyCore, runtime::{Lockable, Runtime}}, transport_adapter::Driver};

pub struct QuicAdapter<F: QuicFactory, R: Runtime> {
    factory: Arc<F>,
    rt: Arc<R>,
    core: R::Mutex<IggyCore>,
}

impl<F: QuicFactory + Send + Sync + 'static, R: Runtime> Driver for QuicAdapter<F, R> {
    fn send_with_response<'a, T: iggy_common::Command>(&'a self, command: &'a T) -> Pin<Box<dyn Future<Output = Result<bytes::Bytes, iggy_common::IggyError>> + Send + 'a>> {
        Box::pin(async move {
            self.core.lock().await.write(command)?;

            loop {
                // опрашиваем core на момент того, не записала ли таска драйвера туда новые данные.
                // То есть вызываем poll в цикле(или wake). При этом чтобы данные не перепутались мы
                // можем сохранять serial id здесь при write, так и драйвер будет знать id, и тут тоже мы будем понимать что ждать.
                // То есть тогда драйвер заисывает payload: Bytes + id: uint64
            }

            Ok(Bytes::new())
        })
    }
}



/*
// tood для transprot
    fn send_with_response<'a, T: iggy_common::Command>(&'a self, command: &'a T) -> Pin<Box<dyn Future<Output = Result<bytes::Bytes, iggy_common::IggyError>> + Send + 'a>> {
        Box::pin(async move {
            self.core.lock().await.write(command)?;

            Ok(Bytes::new())
        })
    }

*/