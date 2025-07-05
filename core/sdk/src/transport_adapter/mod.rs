pub mod quic;

use std::pin::Pin;

use bytes::Bytes;
use iggy_common::{Command, IggyError};

pub trait Driver {
    fn send_with_response<'a, T: Command>(&'a self, command: &'a T) -> Pin<Box<dyn Future<Output = Result<Bytes, IggyError>> + Send + 'a>>;
}
