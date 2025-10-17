/* Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

// Re-export the run_server function from main.rs for in-process testing
// This allows integration tests to run the server without spawning a separate process

use crate::args::Args;
use crate::bootstrap::{
    create_directories, create_shard_connections, create_shard_executor, load_config, load_streams,
    load_users, resolve_persister, update_system_info,
};
use crate::configs::config_provider;
use crate::configs::sharding::CpuAllocation;
use crate::io::fs_utils;
use crate::log::logger::Logging;
use crate::server_error::ServerError;
use crate::shard::namespace::IggyNamespace;
use crate::shard::system::info::SystemInfo;
use crate::shard::transmission::connector::StopSender;
use crate::shard::{IggyShard, ShardInfo, calculate_shard_assignment};
use crate::slab::traits_ext::{
    EntityComponentSystem, EntityComponentSystemMutCell, IntoComponents,
};
use crate::state::file::FileState;
use crate::state::system::SystemState;
use crate::streaming::diagnostics::metrics::Metrics;
use crate::streaming::storage::SystemStorage;
use crate::streaming::utils::MemoryPool;
use crate::streaming::utils::ptr::EternalPtr;
use crate::versioning::SemanticVersion;
use crate::{map_toggle_str, shard_info};
use dashmap::DashMap;
use dotenvy::dotenv;
use error_set::ErrContext;
use iggy_common::{Aes256GcmEncryptor, EncryptorKind, IggyError};
use std::collections::HashSet;
use std::rc::Rc;
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread::JoinHandle;
use tracing::{error, info, instrument, warn};

const COMPONENT: &str = "MAIN";
const SHARDS_TABLE_CAPACITY: usize = 16384;

static SHUTDOWN_START_TIME: AtomicU64 = AtomicU64::new(0);

/// Handle to control a running server instance.
/// Contains shutdown channels and thread handles for graceful termination.
pub struct ServerHandle {
    /// Shutdown senders for each shard (shard_id, sender)
    pub shutdown_senders: Vec<(u16, StopSender)>,
    /// Thread handles for each shard
    pub shard_handles: Vec<JoinHandle<()>>,
    /// Logging instance - must be kept alive to ensure log output is not lost
    _logging: Option<Logging>,
}

impl std::fmt::Debug for ServerHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ServerHandle")
            .field("shutdown_senders", &self.shutdown_senders)
            .field("shard_handles", &self.shard_handles)
            .field("_logging", &"<Logging>")
            .finish()
    }
}

impl ServerHandle {
    /// Triggers graceful shutdown of all shards.
    /// Sends shutdown signal to each shard and waits for threads to complete.
    pub fn shutdown(self) -> Result<(), ServerError> {
        let start_time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        SHUTDOWN_START_TIME.store(start_time, Ordering::SeqCst);

        info!("Initiating graceful shutdown via ServerHandle...");

        // Send shutdown signal to all shards
        for (shard_id, stop_sender) in &self.shutdown_senders {
            if let Err(e) = stop_sender.try_send(()) {
                error!(
                    "Failed to send shutdown signal to shard {}: {}",
                    shard_id, e
                );
            }
        }

        // Wait for all shard threads to complete
        for (idx, handle) in self.shard_handles.into_iter().enumerate() {
            handle
                .join()
                .unwrap_or_else(|_| panic!("Failed to join shard thread-{}", idx));
        }

        let shutdown_duration_msg = {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64;
            let elapsed = now - start_time;
            format!(" (shutdown took {} ms)", elapsed)
        };

        info!(
            "All shards have shut down. Server exiting.{}",
            shutdown_duration_msg
        );

        Ok(())
    }
}

/// Runs the Iggy server with the provided arguments.
/// This function is the main entry point for both the binary and in-process testing.
///
/// # Arguments
/// * `args` - Command-line arguments parsed from `Args`
/// * `install_signal_handler` - If true, installs SIGTERM/SIGINT handler and blocks until shutdown.
///                               If false, returns ServerHandle immediately for programmatic control.
///
/// # Returns
/// * `Ok(ServerHandle)` containing shutdown channels and thread handles
/// * `Err(ServerError)` if there was an error during startup or runtime
#[instrument(skip_all, name = "trace_start_server")]
pub async fn run_server(
    args: Args,
    install_signal_handler: bool,
) -> Result<ServerHandle, ServerError> {
    if let Ok(env_path) = std::env::var("IGGY_ENV_PATH") {
        if dotenvy::from_path(&env_path).is_ok() {
            println!("Loaded environment variables from path: {env_path}");
        }
    } else if let Ok(path) = dotenv() {
        println!(
            "Loaded environment variables from .env file at path: {}",
            path.display()
        );
    }

    // FIRST DISCRETE LOADING STEP.
    // Load config and create directories.
    // Remove `local_data` directory if run with `--fresh` flag.
    let config = if let Some(config) = args.config {
        // Use provided config directly (test mode)
        config
    } else {
        // Load config from file (normal mode)
        let config_provider = config_provider::resolve(&args.config_provider)?;
        load_config(&config_provider)
            .await
            .with_error_context(|error| {
                format!("{COMPONENT} (error: {error}) - failed to load config during bootstrap")
            })?
    };
    if args.fresh {
        let system_path = config.system.get_system_path();
        if compio::fs::metadata(&system_path).await.is_ok() {
            println!(
                "Removing system path at: {} because `--fresh` flag was set",
                system_path
            );
            if let Err(e) = fs_utils::remove_dir_all(&system_path).await {
                eprintln!("Failed to remove system path at {}: {}", system_path, e);
            }
        }
    }

    // SECOND DISCRETE LOADING STEP.
    // Create directories.
    create_directories(&config.system).await?;

    // Initialize logging
    // THIRD DISCRETE LOADING STEP.
    let mut logging = Logging::new(config.telemetry.clone());
    let initialized = logging.early_init();

    // From this point on, we can use tracing macros to log messages.
    // Skip late_init if subscriber was already initialized (e.g., in test mode)
    if initialized {
        logging.late_init(config.system.get_system_path(), &config.system.logging)?;
    }

    if args.with_default_root_credentials {
        let username_set = std::env::var("IGGY_ROOT_USERNAME").is_ok();
        let password_set = std::env::var("IGGY_ROOT_PASSWORD").is_ok();

        if !username_set || !password_set {
            if !username_set {
                unsafe {
                    std::env::set_var("IGGY_ROOT_USERNAME", "iggy");
                }
            }
            if !password_set {
                unsafe {
                    std::env::set_var("IGGY_ROOT_PASSWORD", "iggy");
                }
            }
            info!(
                "Using default root credentials (username: iggy, password: iggy) - FOR DEVELOPMENT ONLY!"
            );
        } else {
            warn!(
                "--with-default-root-credentials flag is ignored because root credentials are already set via environment variables"
            );
        }
    }

    // FOURTH DISCRETE LOADING STEP.
    MemoryPool::init_pool(config.system.clone());

    // SIXTH DISCRETE LOADING STEP.
    let partition_persister = resolve_persister(config.system.partition.enforce_fsync);
    let storage = SystemStorage::new(config.system.clone(), partition_persister);

    // SEVENTH DISCRETE LOADING STEP.
    let current_version = SemanticVersion::current().expect("Invalid version");
    let mut system_info;
    let load_system_info = storage.info.load().await;
    match load_system_info {
        Ok(info) => {
            system_info = info;
        }
        Err(e) => {
            if let IggyError::ResourceNotFound(_) = e {
                info!("System info not found, creating...");
                system_info = SystemInfo::default();
                update_system_info(&storage, &mut system_info, &current_version).await?;
            } else {
                panic!("Failed to load system info from disk. {e}");
            }
        }
    }
    info!("Loaded {system_info}.");
    let loaded_version = SemanticVersion::from_str(&system_info.version.version)?;
    if current_version.is_equal_to(&loaded_version) {
        info!("System version {current_version} is up to date.");
    } else if current_version.is_greater_than(&loaded_version) {
        info!(
            "System version {current_version} is greater than {loaded_version}, checking the available migrations..."
        );
        update_system_info(&storage, &mut system_info, &current_version).await?;
    } else {
        info!(
            "System version {current_version} is lower than {loaded_version}, possible downgrade."
        );
        update_system_info(&storage, &mut system_info, &current_version).await?;
    }

    // EIGHTH DISCRETE LOADING STEP.
    info!(
        "Server-side encryption is {}.",
        map_toggle_str(config.system.encryption.enabled)
    );
    let encryptor: Option<EncryptorKind> = match config.system.encryption.enabled {
        true => Some(EncryptorKind::Aes256Gcm(
            Aes256GcmEncryptor::from_base64_key(&config.system.encryption.key).unwrap(),
        )),
        false => None,
    };

    // TENTH DISCRETE LOADING STEP.
    let state_persister = resolve_persister(config.system.state.enforce_fsync);
    let state = FileState::new(
        &config.system.get_state_messages_file_path(),
        &current_version,
        state_persister,
        encryptor.clone(),
    );
    let state = SystemState::load(state).await?;
    let (streams_state, users_state) = state.decompose();
    let streams = load_streams(streams_state.into_values(), &config.system).await?;
    let users = load_users(users_state.into_values());

    // ELEVENTH DISCRETE LOADING STEP.
    let shards_set = config.system.sharding.cpu_allocation.to_shard_set();
    match &config.system.sharding.cpu_allocation {
        CpuAllocation::All => {
            info!(
                "Using all available CPU cores ({} shards with affinity)",
                shards_set.len()
            );
        }
        CpuAllocation::Count(count) => {
            info!("Using {count} shards with affinity to cores 0..{count}");
        }
        CpuAllocation::Range(start, end) => {
            info!(
                "Using {} shards with affinity to cores {start}..{end}",
                end - start
            );
        }
    }

    #[cfg(feature = "disable-mimalloc")]
    warn!("Using default system allocator because code was build with `disable-mimalloc` feature");
    #[cfg(not(feature = "disable-mimalloc"))]
    info!("Using mimalloc allocator");

    // DISCRETE STEP.
    // Increment the metrics.
    let metrics = Metrics::init();

    // TWELFTH DISCRETE LOADING STEP.
    info!("Starting {} shard(s)", shards_set.len());
    let (connections, shutdown_handles) = create_shard_connections(&shards_set);
    let mut handles = Vec::with_capacity(shards_set.len());

    // TODO: Persist the shards table and load it from the disk, so it does not have to be
    // THIRTEENTH DISCRETE LOADING STEP.
    // Shared resources bootstrap.
    let shards_table = Box::new(DashMap::with_capacity(SHARDS_TABLE_CAPACITY));
    let shards_table = Box::leak(shards_table);
    let shards_table: EternalPtr<DashMap<IggyNamespace, ShardInfo>> = shards_table.into();
    streams.with_components(|components| {
        let (root, ..) = components.into_components();
        for (_, stream) in root.iter() {
            stream.topics().with_components(|components| {
                let (root, ..) = components.into_components();
                for (_, topic) in root.iter() {
                    topic.partitions().with_components(|components| {
                        let (root, ..) = components.into_components();
                        for (_, partition) in root.iter() {
                            let stream_id = stream.id();
                            let topic_id = topic.id();
                            let partition_id = partition.id();
                            let ns = IggyNamespace::new(stream_id, topic_id, partition_id);
                            let shard_id = calculate_shard_assignment(&ns, shards_set.len() as u32);
                            let shard_info = ShardInfo::new(shard_id);
                            shards_table.insert(ns, shard_info);
                        }
                    });
                }
            })
        }
    });

    for (id, cpu_id) in shards_set
        .into_iter()
        .enumerate()
        .map(|(idx, cpu)| (idx as u16, cpu))
    {
        let streams = streams.clone();
        let shards_table = shards_table.clone();
        let users = users.clone();
        let connections = connections.clone();
        let config = config.clone();
        let encryptor = encryptor.clone();
        let metrics = metrics.clone();
        let state_persister = resolve_persister(config.system.state.enforce_fsync);
        let state = FileState::new(
            &config.system.get_state_messages_file_path(),
            &current_version,
            state_persister,
            encryptor.clone(),
        );

        // TODO: Explore decoupling the `Log` from `Partition` entity.
        // Ergh... I knew this will backfire to include `Log` as part of the `Partition` entity,
        // We have to initialize with a default log for every partition, once we `Clone` the Streams / Topics / Partitions,
        // because `Clone` impl for `Partition` does not clone the actual log, just creates an empty one.
        streams.with_components(|components| {
            let (root, ..) = components.into_components();
            for (_, stream) in root.iter() {
                stream.topics().with_components_mut(|components| {
                    let (mut root, ..) = components.into_components();
                    for (_, topic) in root.iter_mut() {
                        let partitions_count = topic.partitions().len();
                        for log_id in 0..partitions_count {
                            let id = topic.partitions_mut().insert_default_log();
                            assert_eq!(
                                id, log_id,
                                "main: partition_insert_default_log: id mismatch when creating default log"
                            );
                        }
                    }
                })
            }
        });

        let handle = std::thread::Builder::new()
            .name(format!("shard-{id}"))
            .spawn(move || {
                let affinity_set = HashSet::from([cpu_id]);
                let rt = create_shard_executor(affinity_set);
                rt.block_on(async move {
                    let builder = IggyShard::builder();
                    let shard = builder
                        .id(id)
                        .streams(streams)
                        .state(state)
                        .users(users)
                        .shards_table(shards_table)
                        .connections(connections)
                        .config(config)
                        .encryptor(encryptor)
                        .version(current_version)
                        .metrics(metrics)
                        .build();
                    let shard = Rc::new(shard);

                    if let Err(e) = shard.run().await {
                        error!("Failed to run shard-{id}: {e}");
                    }
                    shard_info!(shard.id, "Run completed");

                    // Drop the shard Rc here to ensure the IggyShard is dropped on the shard thread
                    // before the thread exits. This prevents cross-thread drops of AtomicWakers
                    // that may contain SendWrapper types from HTTP handlers.
                    drop(shard);
                })
            })
            .unwrap_or_else(|e| panic!("Failed to spawn thread for shard-{id}: {e}"));
        handles.push(handle);
    }

    if install_signal_handler {
        // Binary mode: install signal handler and block until shutdown
        let shutdown_handles_for_signal = shutdown_handles.clone();
        ctrlc::set_handler(move || {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64;
            SHUTDOWN_START_TIME.store(now, Ordering::SeqCst);

            info!("Received shutdown signal (SIGTERM/SIGINT), initiating graceful shutdown...");

            for (shard_id, stop_sender) in &shutdown_handles_for_signal {
                if let Err(e) = stop_sender.try_send(()) {
                    error!(
                        "Failed to send shutdown signal to shard {}: {}",
                        shard_id, e
                    );
                }
            }
        })
        .expect("Error setting Ctrl-C handler");

        info!("Iggy server is running. Press Ctrl+C or send SIGTERM to shutdown.");

        // Block until all shards complete
        for (idx, handle) in handles.into_iter().enumerate() {
            handle
                .join()
                .unwrap_or_else(|_| panic!("Failed to join shard thread-{}", idx));
        }

        let shutdown_duration_msg = {
            let start_time = SHUTDOWN_START_TIME.load(Ordering::SeqCst);
            if start_time > 0 {
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as u64;
                let elapsed = now - start_time;
                format!(" (shutdown took {} ms)", elapsed)
            } else {
                String::new()
            }
        };

        info!(
            "All shards have shut down. Iggy server is exiting.{}",
            shutdown_duration_msg
        );

        // Return empty handle since shutdown already completed
        // Logging is dropped here, which flushes remaining logs
        Ok(ServerHandle {
            shutdown_senders: Vec::new(),
            shard_handles: Vec::new(),
            _logging: None,
        })
    } else {
        // Test mode: return handle for programmatic shutdown
        // Keep logging alive so shard logs remain visible during tests
        Ok(ServerHandle {
            shutdown_senders: shutdown_handles,
            shard_handles: handles,
            _logging: Some(logging),
        })
    }
}
