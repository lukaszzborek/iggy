use bon::Builder;
use iggy_common::IggyDuration;

/// Configuration for the *synchronous* (blocking) producer.
#[derive(Clone, Builder)]
pub struct DirectConfig {
    /// Maximum number of messages to pack into **one** synchronous request.
    /// `0` => MAX_BATCH_LENTH().
    #[builder(default = 1000)]
    pub batch_length: u32,
    /// How long to wait for more messages before flushing the current set.
    #[builder(default = IggyDuration::from(1000))]
    pub linger_time: IggyDuration,
}
