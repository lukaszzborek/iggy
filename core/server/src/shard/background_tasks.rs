use iggy_common::{Identifier, IggyError, IggyTimestamp};
use std::rc::Rc;
use tracing::{debug, error, info, trace, warn};

use super::IggyShard;

pub async fn save_messages(shard: Rc<IggyShard>) -> Result<(), IggyError> {
    let config = &shard.config.message_saver;
    if !config.enabled {
        return Ok(());
    }

    trace!("Saving buffered messages...");

    let namespaces = shard.get_current_shard_namespaces();
    let mut total_saved_messages = 0u32;
    let reason = "background saver triggered".to_string();

    for ns in namespaces {
        let stream_id = Identifier::numeric(ns.stream_id() as u32).unwrap();
        let topic_id = Identifier::numeric(ns.topic_id() as u32).unwrap();
        let partition_id = ns.partition_id();

        match shard
            .streams2
            .persist_messages(
                shard.id,
                &stream_id,
                &topic_id,
                partition_id,
                reason.clone(),
                &shard.config.system,
            )
            .await
        {
            Ok(batch_count) => {
                total_saved_messages += batch_count;
            }
            Err(err) => {
                error!(
                    "Failed to save messages for partition {}: {}",
                    partition_id, err
                );
            }
        }
    }

    if total_saved_messages > 0 {
        info!("Saved {} buffered messages on disk.", total_saved_messages);
    }

    Ok(())
}

pub async fn verify_heartbeats(shard: Rc<IggyShard>) -> Result<(), IggyError> {
    const MAX_THRESHOLD: f64 = 1.2;

    let config = &shard.config.heartbeat;
    if !config.enabled {
        return Ok(());
    }

    let interval = config.interval;
    let max_interval =
        iggy_common::IggyDuration::from((MAX_THRESHOLD * interval.as_micros() as f64) as u64);

    trace!("Verifying heartbeats...");

    let clients = {
        let client_manager = shard.client_manager.borrow();
        client_manager.get_clients()
    };

    let current_timestamp = IggyTimestamp::now();

    for client in clients {
        let last_heartbeat = client.last_heartbeat;
        let elapsed_time = current_timestamp.as_micros() - last_heartbeat.as_micros();
        if elapsed_time > max_interval.as_micros() {
            warn!(
                "Client: {} heartbeat has not been received for: {} μs, the max allowed interval is: {} μs",
                client.session.client_id,
                elapsed_time,
                max_interval.as_micros()
            );
            let mut client_manager = shard.client_manager.borrow_mut();
            client_manager.delete_client(client.session.client_id);
        }
    }

    Ok(())
}

pub async fn clean_personal_access_tokens(shard: Rc<IggyShard>) -> Result<(), IggyError> {
    let config = &shard.config.personal_access_token.cleaner;
    if !config.enabled {
        return Ok(());
    }

    trace!("Cleaning expired personal access tokens...");

    let users = shard.users.borrow();
    let current_timestamp = IggyTimestamp::now();
    let mut total_deleted_tokens = 0u32;

    for user in users.values() {
        let tokens_to_delete: Vec<_> = user
            .personal_access_tokens
            .iter()
            .filter(|token| {
                if let Some(expiry) = token.expiry_at {
                    current_timestamp.as_micros() > expiry.as_micros()
                } else {
                    false
                }
            })
            .map(|token| token.name.clone())
            .collect();

        for token_name in tokens_to_delete {
            if let Err(err) = shard.delete_personal_access_token_bypass_auth(user.id, &token_name) {
                error!(
                    "Failed to delete expired personal access token: {} for user: {}, error: {}",
                    token_name, user.id, err
                );
            } else {
                debug!(
                    "Deleted expired personal access token: {} for user: {}",
                    token_name, user.id
                );
                total_deleted_tokens += 1;
            }
        }
    }

    if total_deleted_tokens > 0 {
        info!(
            "Deleted {} expired personal access tokens.",
            total_deleted_tokens
        );
    }

    Ok(())
}

pub async fn print_sysinfo(shard: Rc<IggyShard>) -> Result<(), IggyError> {
    if shard.config.system.logging.sysinfo_print_interval.is_zero() || shard.id != 0 {
        return Ok(());
    }

    trace!("Printing system information...");

    let stats = shard.get_stats().await?;
    info!("{:?}", stats);

    Ok(())
}
