//! Prelude module for the Iggy SDK.
//!
//! This module re-exports the most common types, traits, and functions
//! from the Iggy SDK to make them easier to import and use.
//!
//! # Examples
//!
//! ```
//! use iggy::prelude::*;
//! ```

// TODO(hubcio): finish this

pub use crate::bytes_serializable::BytesSerializable;
pub use crate::client::Client;
pub use crate::error::IggyError;
pub use crate::identifier::Identifier;
pub use crate::messages::{
    FlushUnsavedBuffer, Partitioning, PollMessages, PollingKind, PollingStrategy, SendMessages,
};
pub use crate::models::partition::Partition;
pub use crate::models::stream::Stream;
pub use crate::models::topic::Topic;
pub use crate::models::{
    IggyMessage, IggyMessageHeader, IggyMessageView, IggyMessageViewIterator, IggyMessageViewMut,
    IggyMessageViewMutIterator, IggyMessages, IggyMessagesMut,
};
pub use crate::utils::byte_size::IggyByteSize;
pub use crate::utils::sizeable::Sizeable;
pub use crate::utils::timestamp::IggyTimestamp;
pub use crate::validatable::Validatable;
