use std::pin::Pin;

use iggy_common::IggyError;

pub mod tcp;
pub mod quic;

pub trait ConnectionFactory {
    fn connect(&self) -> Pin<Box<dyn Future<Output = Result<(), IggyError>> + Send + Sync>>;
    fn is_alive(&self) -> Pin<Box<dyn Future<Output = bool>>>;
    fn shutdown(&self) -> Pin<Box<dyn Future<Output = Result<(), IggyError>> + Send + Sync>>;
}
