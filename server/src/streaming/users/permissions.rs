use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Serialize, Deserialize)]
pub struct Permissions {
    pub global: GlobalPermissions,
    pub streams: Option<HashMap<u32, StreamPermissions>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct GlobalPermissions {
    pub manage_servers: bool,
    pub read_servers: bool,
    pub manage_users: bool,
    pub read_users: bool,
    pub manage_streams: bool,
    pub read_streams: bool,
    pub manage_topics: bool,
    pub read_topics: bool,
    pub poll_messages: bool,
    pub send_messages: bool,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct StreamPermissions {
    pub manage_stream: bool,
    pub read_stream: bool,
    pub manage_topics: bool,
    pub read_topics: bool,
    pub poll_messages: bool,
    pub send_messages: bool,
    pub topics: Option<HashMap<u32, TopicPermissions>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TopicPermissions {
    pub manage_topic: bool,
    pub read_topic: bool,
    pub poll_messages: bool,
    pub send_messages: bool,
}

impl Permissions {
    pub fn root() -> Self {
        Self {
            global: GlobalPermissions {
                manage_servers: true,
                read_servers: true,
                manage_users: true,
                read_users: true,
                manage_streams: true,
                read_streams: true,
                manage_topics: true,
                read_topics: true,
                poll_messages: true,
                send_messages: true,
            },
            streams: None,
        }
    }
}
