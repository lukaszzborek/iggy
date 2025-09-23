use async_channel::Receiver;
use bytes::BytesMut;
use compio::net::{TcpListener as CompioTcpListener, TcpOpts, TcpStream};
use error_set::ErrContext;
use futures::FutureExt;
use iggy_common::{IggyError, TransportProtocol};
use std::cell::RefCell;
use std::net::SocketAddr;
use std::rc::Rc;
use std::time::Duration;
use tracing::{debug, error, info, warn};

use crate::binary::command::{ServerCommand, ServerCommandHandler};
use crate::binary::sender::SenderKind;
use crate::configs::tcp::TcpSocketConfig;
use crate::shard::listener::{ConnectionController, ConnectionHandle, Listener};
use crate::shard::transmission::event::ShardEvent;
use crate::shard::{IggyShard, shard_error, shard_info};
use crate::streaming::session::Session;

const INITIAL_BYTES_LENGTH: usize = 4;

pub struct TcpListener {
    address: String,
    config: TcpSocketConfig,
}

impl TcpListener {
    pub fn new(address: String, config: TcpSocketConfig) -> Self {
        Self { address, config }
    }
}

impl Listener for TcpListener {
    fn name(&self) -> &'static str {
        "tcp_listener"
    }

    async fn run(
        &mut self,
        shard: Rc<IggyShard>,
        shutdown: Receiver<()>,
        connections: Rc<RefCell<Vec<ConnectionHandle>>>,
        is_shutting_down: Rc<RefCell<bool>>,
    ) -> Result<(), IggyError> {
        let mut addr: SocketAddr = self
            .address
            .parse()
            .map_err(|_| IggyError::InvalidServerAddress)?;

        if shard.id != 0 && addr.port() == 0 {
            shard_info!(shard.id, "Waiting for TCP address from shard 0...");
            loop {
                futures::select! {
                    _ = shutdown.recv().fuse() => {
                        return Ok(());
                    }
                    _ = compio::time::sleep(Duration::from_millis(10)).fuse() => {
                        if let Some(bound_addr) = shard.tcp_bound_address.get() {
                            addr = bound_addr;
                            shard_info!(shard.id, "Received TCP address: {}", addr);
                            break;
                        }
                    }
                }
            }
        }

        let listener = create_tcp_listener(addr, &self.config)
            .await
            .map_err(|_| IggyError::CannotBindToSocket(addr.to_string()))
            .with_error_context(|err| {
                format!("Failed to bind TCP server to address: {addr}, {err}")
            })?;

        let actual_addr = listener.local_addr().map_err(|e| {
            shard_error!(shard.id, "Failed to get local address: {e}");
            IggyError::CannotBindToSocket(addr.to_string())
        })?;

        shard_info!(shard.id, "TCP server has started on: {:?}", actual_addr);

        if shard.id == 0 {
            if addr.port() == 0 {
                let event = ShardEvent::TcpBound {
                    address: actual_addr,
                };
                shard.broadcast_event_to_all_shards(event).await;
            }

            let mut current_config = shard.config.clone();
            current_config.tcp.address = actual_addr.to_string();

            let runtime_path = current_config.system.get_runtime_path();
            let current_config_path = format!("{runtime_path}/current_config.toml");
            let current_config_content =
                toml::to_string(&current_config).expect("Cannot serialize current_config");

            let buf_result = compio::fs::write(&current_config_path, current_config_content).await;
            match buf_result.0 {
                Ok(_) => shard_info!(
                    shard.id,
                    "Current config written to: {}",
                    current_config_path
                ),
                Err(e) => shard_error!(
                    shard.id,
                    "Failed to write current config to {}: {}",
                    current_config_path,
                    e
                ),
            }
        }

        loop {
            futures::select! {
                _ = shutdown.recv().fuse() => {
                    shard_info!(shard.id, "TCP server shutting down, no longer accepting connections");
                    break;
                }

                result = listener.accept().fuse() => {
                    if *is_shutting_down.borrow() {
                        if let Ok((stream, address)) = result {
                            shard_info!(shard.id, "Rejecting new connection from {} during shutdown", address);
                            drop(stream);
                        }
                        continue;
                    }

                    match result {
                        Ok((stream, address)) => {
                            shard_info!(shard.id, "Accepted new TCP connection: {}", address);
                            self.handle_new_connection(stream, address, &shard, &connections).await;
                        }
                        Err(error) => shard_error!(shard.id, "Unable to accept TCP socket: {}", error),
                    }
                }
            }
        }

        Ok(())
    }
}

impl TcpListener {
    async fn handle_new_connection(
        &self,
        stream: TcpStream,
        addr: SocketAddr,
        shard: &Rc<IggyShard>,
        connections: &Rc<RefCell<Vec<ConnectionHandle>>>,
    ) {
        let transport = TransportProtocol::Tcp;
        let session = shard.add_client(&addr, transport);
        let client_id = session.client_id;

        shard_info!(
            shard.id,
            "Added {} client with session: {} for IP address: {}",
            transport,
            session,
            addr
        );

        shard.add_active_session(session.clone());
        let event = ShardEvent::NewSession {
            address: addr,
            transport,
        };
        let _responses = shard.broadcast_event_to_all_shards(event).await;

        let (handle, controller) = ConnectionHandle::new(client_id);
        connections.borrow_mut().push(handle);

        let sender = SenderKind::get_tcp_sender(stream);
        let shard_clone = Rc::clone(shard);
        let connections_clone = connections.clone();

        compio::runtime::spawn(async move {
            info!("TCP connection {} started", client_id);

            handle_tcp_connection(session, sender, shard_clone, controller).await;

            // Clean up connection
            connections_clone
                .borrow_mut()
                .retain(|h| h.client_id() != client_id);
            info!("TCP connection {} ended", client_id);
        });
    }
}

async fn handle_tcp_connection(
    session: Rc<Session>,
    mut sender: SenderKind,
    shard: Rc<IggyShard>,
    controller: ConnectionController,
) {
    let client_id = controller.client_id();
    let mut length_buffer = BytesMut::with_capacity(INITIAL_BYTES_LENGTH);
    let mut code_buffer = BytesMut::with_capacity(INITIAL_BYTES_LENGTH);

    loop {
        if controller.is_shutdown_requested() {
            info!("Connection {} received shutdown signal", client_id);
            let _ = sender.send_error_response(IggyError::Disconnected).await;
            break;
        }

        let read_future = sender.read(length_buffer.clone());
        let (result, mut initial_buffer) = futures::select! {
            _ = controller.wait_shutdown().fuse() => {
                info!("Connection stop signal received for session: {}", session);
                let _ = sender.send_error_response(IggyError::Disconnected).await;
                break;
            }
            result = read_future.fuse() => {
                result
            }
        };

        match result {
            Ok(_) => {}
            Err(error) => {
                if error.as_code() == IggyError::ConnectionClosed.as_code() {
                    break;
                } else {
                    error!("Connection error: {:?}", error);
                    let _ = sender.send_error_response(error).await;
                    continue;
                }
            }
        }

        let length =
            u32::from_le_bytes(initial_buffer[0..INITIAL_BYTES_LENGTH].try_into().unwrap());
        let (res, mut code_buffer_out) = sender.read(code_buffer.clone()).await;
        if let Err(e) = res {
            error!("Failed to read command code: {}", e);
            break;
        }
        let code = u32::from_le_bytes(code_buffer_out[0..INITIAL_BYTES_LENGTH].try_into().unwrap());

        initial_buffer.clear();
        code_buffer_out.clear();
        length_buffer = initial_buffer;
        code_buffer = code_buffer_out;

        debug!("Received TCP request, length: {}, code: {}", length, code);

        let command = match ServerCommand::from_code_and_reader(code, &mut sender, length - 4).await
        {
            Ok(cmd) => cmd,
            Err(e) => {
                error!("Failed to parse command: {}", e);
                let _ = sender.send_error_response(e).await;
                continue;
            }
        };

        match command.handle(&mut sender, length, &session, &shard).await {
            Ok(_) => {
                debug!("Command handled successfully for session: {}", session);
            }
            Err(error) => {
                error!("Command failed for session: {}, error: {}", session, error);
                if let IggyError::ClientNotFound(_) = error {
                    let _ = sender.send_error_response(error).await;
                    break;
                } else {
                    let _ = sender.send_error_response(error).await;
                }
            }
        }
    }

    controller.notify_closed();
}

async fn create_tcp_listener(
    addr: SocketAddr,
    config: &TcpSocketConfig,
) -> Result<CompioTcpListener, std::io::Error> {
    let opts = TcpOpts::new().reuse_port(true).reuse_port(true);
    let opts = if config.override_defaults {
        let recv_buffer_size = config
            .recv_buffer_size
            .as_bytes_u64()
            .try_into()
            .expect("Failed to parse recv_buffer_size for TCP socket");

        let send_buffer_size = config
            .send_buffer_size
            .as_bytes_u64()
            .try_into()
            .expect("Failed to parse send_buffer_size for TCP socket");

        opts.recv_buffer_size(recv_buffer_size)
            .send_buffer_size(send_buffer_size)
            .keepalive(config.keepalive)
            .linger(config.linger.get_duration())
            .nodelay(config.nodelay)
    } else {
        opts
    };
    CompioTcpListener::bind_with_options(addr, opts).await
}
