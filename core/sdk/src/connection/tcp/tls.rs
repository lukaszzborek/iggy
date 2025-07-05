use std::{io, net::SocketAddr, pin::Pin};

use rustls::pki_types::ServerName;
use tokio_util::compat::{Compat, TokioAsyncReadCompatExt};

use crate::connection::tcp::{AsyncStream, SocketFactory};

// pub trait TlsConnector<Plain: AsyncStream> {
//     type TlsStream: AsyncStream;

//     fn handshake(
//         &self,
//         plain: Plain,
//         domain: &str,
//     ) -> Pin<Box<dyn Future<Output = io::Result<Self::TlsStream>> + Send>>;
// }

// pub struct TokioRustls {
//     inner: tokio_rustls::TlsConnector,
// }
// impl TlsConnector<Compat<tokio::net::TcpStream>> for TokioRustls {
//     type TlsStream = Compat<tokio_rustls::client::TlsStream<tokio::net::TcpStream>>;

//     fn handshake(
//         &self,
//         plain: Compat<tokio::net::TcpStream>,
//         domain: &str,
//     ) -> Pin<Box<dyn Future<Output = std::io::Result<Self::TlsStream>> + Send>> {
//         let dom = ServerName::try_from(domain.to_owned()).unwrap();
//         let tcp = plain.into_inner();
//         let fut = self.inner.connect(dom, tcp);
//         Box::pin(async move { Ok(fut.await?.compat()) })
//     }
// }

// pub struct TlsFactory<F, C> {
//     base: F,
//     tls:  C,
//     domain: String,
// }

// impl<F, C> SocketFactory for TlsFactory<F, C>
// where
//     F: SocketFactory,
//     C: TlsConnector<F::Stream> + Clone + Send + Sync + 'static,
// {
//     type Stream = C::TlsStream;

//     fn connect(&self) -> Pin<Box<dyn Future<Output = io::Result<Self::Stream>> + Send>> {
//         let base = self.base.connect(addr);
//         let tls  = self.tls.clone();
//         let dom  = self.domain.clone();
//         Box::pin(async move {
//             let plain = base.await?;
//             tls.handshake(plain, &dom).await
//         })
//     }
// }
