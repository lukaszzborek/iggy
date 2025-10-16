use super::{BalancedSharding, ErrorCallback, LogErrorCallback, Sharding};
use crate::clients::MIB;
use bon::Builder;
use iggy_common::{IggyByteSize, IggyDuration};
use std::sync::Arc;

/// Determines how the `send_messages` API should behave when problem is encountered
#[derive(Debug, Clone)]
pub enum BackpressureMode {
    /// Block until the send succeeds
    Block,
    /// Block with a timeout, after which the send fails
    BlockWithTimeout(IggyDuration),
    /// Fail immediately without retrying
    FailImmediately,
}

/// Configuration for the *background* (asynchronous) producer
#[derive(Debug, Builder)]
pub struct BackgroundConfig {
    /// Number of shard-workers that run in parallel.
    #[builder(default = default_shard_count())]
    pub num_shards: usize,
    /// How long a shard may wait before flushing an *incomplete* batch.
    #[builder(default = IggyDuration::from(1000))]
    pub linger_time: IggyDuration,
    /// User-supplied asynchronous callback that will be executed whenever
    /// the producer encounters an error it cannot automatically recover from
    #[builder(default = Arc::new(Box::new(LogErrorCallback)))]
    pub error_callback: Arc<Box<dyn ErrorCallback + Send + Sync>>,
    /// Strategy that maps a message to a shard.
    #[builder(default = Box::new(BalancedSharding::default()))]
    pub sharding: Box<dyn Sharding + Send + Sync>,
    /// Maximum **total size in bytes** of a batch. `0` => unlimited.
    #[builder(default = MIB)]
    pub batch_size: usize,
    /// Maximum **number of messages** per batch. `0` => unlimited.
    #[builder(default = 1000)]
    pub batch_length: usize,
    /// Action to apply when back-pressure limits are reached
    #[builder(default = BackpressureMode::Block)]
    pub failure_mode: BackpressureMode,
    /// Upper bound for the **bytes held in memory** across *all* shards.
    #[builder(default = IggyByteSize::from(32 * MIB as u64))]
    pub max_buffer_size: IggyByteSize,
    /// Maximum number of **in-flight requests** (batches being sent). `0` => unlimited.
    #[builder(default = default_shard_count() * 2)]
    pub max_in_flight: usize,
}

fn default_shard_count() -> usize {
    let cpus = num_cpus::get();
    cpus.clamp(2, 64)
}
