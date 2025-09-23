use async_channel::Receiver;
use futures::{FutureExt, StreamExt};
use iggy_common::IggyError;
use std::rc::Rc;
use tracing::info;

use super::IggyShard;
use super::transmission::frame::ShardFrame;

pub async fn receive_shard_messages(
    shard: Rc<IggyShard>,
    shutdown: Receiver<()>,
) -> Result<(), IggyError> {
    let messages_receiver = shard.messages_receiver.take();
    if messages_receiver.is_none() {
        return Ok(());
    }

    let mut messages_receiver = messages_receiver.unwrap();

    loop {
        futures::select! {
            _ = shutdown.recv().fuse() => {
                info!("Shard message receiver shutting down");
                shard.messages_receiver.replace(Some(messages_receiver));
                break;
            }
            frame = messages_receiver.next().fuse() => {
                if let Some(frame) = frame {
                    let ShardFrame { message, response_sender } = frame;
                    match (shard.handle_shard_message(message).await, response_sender) {
                        (Some(response), Some(response_sender)) => {
                            response_sender
                                .send(response)
                                .await
                                .expect("Failed to send response back to origin shard.");
                        }
                        _ => {}
                    };
                }
            }
        }
    }

    Ok(())
}
