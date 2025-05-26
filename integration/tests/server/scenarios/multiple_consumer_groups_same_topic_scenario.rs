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

use crate::server::scenarios::{STREAM_ID, STREAM_NAME, TOPIC_ID, TOPIC_NAME, create_client};
use futures::StreamExt;
use iggy::prelude::*;
use integration::test_server::{
    ClientFactory, assert_clean_system, create_user, delete_user, login_root, login_user,
};
use std::str::{FromStr, from_utf8};

const CONSUMER_GROUP_1_NAME: &str = "test-consumer-group-1";
const USER_CG1_NAME: &str = "user_cg1";

const CONSUMER_GROUP_2_NAME: &str = "test-consumer-group-2";
const USER_CG2_NAME: &str = "user_cg2";

const MESSAGES_COUNT: u64 = 11372;
const PARTITIONS_COUNT: u64 = 1;

pub async fn run(client_factory: &dyn ClientFactory) {
    let system_client = create_client(client_factory).await;
    let client_for_cg1_builder = create_client(client_factory).await;
    let client_for_cg2_builder = create_client(client_factory).await;

    login_root(&system_client).await;

    init_base_infrastructure(&system_client).await;

    let (mut consumer1, mut consumer2) = build_consumers_with_builder(
        &client_for_cg1_builder,
        USER_CG1_NAME,
        CONSUMER_GROUP_1_NAME,
        &client_for_cg2_builder,
        USER_CG2_NAME,
        CONSUMER_GROUP_2_NAME,
    )
    .await;

    // Validate group state after consumers are built and potentially joined/created groups
    let cg1_details = get_consumer_group_details(&system_client, CONSUMER_GROUP_1_NAME).await;
    assert_eq!(cg1_details.name, CONSUMER_GROUP_1_NAME);
    assert_eq!(cg1_details.members_count, 1);
    let client_cg1_info = client_for_cg1_builder.get_me().await.unwrap();
    assert_eq!(cg1_details.members[0].id, client_cg1_info.client_id);
    assert_eq!(cg1_details.members[0].partitions_count, PARTITIONS_COUNT);

    let cg2_details = get_consumer_group_details(&system_client, CONSUMER_GROUP_2_NAME).await;
    assert_eq!(cg2_details.name, CONSUMER_GROUP_2_NAME);
    assert_eq!(cg2_details.members_count, 1);
    let client_cg2_info = client_for_cg2_builder.get_me().await.unwrap();
    assert_eq!(cg2_details.members[0].id, client_cg2_info.client_id);
    assert_eq!(cg2_details.members[0].partitions_count, PARTITIONS_COUNT);

    send_messages_and_poll_with_consumers(&system_client, &mut consumer1, &mut consumer2).await;

    cleanup(&system_client).await;
    assert_clean_system(&system_client).await;
}

async fn init_base_infrastructure(system_client: &IggyClient) {
    // 1. Create stream
    system_client
        .create_stream(STREAM_NAME, Some(STREAM_ID))
        .await
        .unwrap();

    // 2. Create topic
    system_client
        .create_topic(
            &Identifier::named(STREAM_NAME).unwrap(),
            TOPIC_NAME,
            PARTITIONS_COUNT,
            CompressionAlgorithm::default(),
            None,
            Some(TOPIC_ID),
            IggyExpiry::NeverExpire,
            MaxTopicSize::ServerDefault,
        )
        .await
        .unwrap();

    // 3. Create users
    create_user(system_client, USER_CG1_NAME).await;
    create_user(system_client, USER_CG2_NAME).await;
}

#[allow(clippy::too_many_arguments)]
async fn build_consumers_with_builder(
    client_for_cg1_builder: &IggyClient,
    user_cg1_name: &str,
    consumer_group_1_name: &str,
    client_for_cg2_builder: &IggyClient,
    user_cg2_name: &str,
    consumer_group_2_name: &str,
) -> (IggyConsumer, IggyConsumer) {
    login_user(client_for_cg1_builder, user_cg1_name).await;
    login_user(client_for_cg2_builder, user_cg2_name).await;

    let mut consumer1 = client_for_cg1_builder
        .consumer_group(consumer_group_1_name, STREAM_NAME, TOPIC_NAME)
        .unwrap()
        .auto_commit(AutoCommit::When(AutoCommitWhen::ConsumingAllMessages))
        .create_consumer_group_if_not_exists()
        .auto_join_consumer_group()
        .polling_strategy(PollingStrategy::next())
        .poll_interval(IggyDuration::from_str("1ms").unwrap())
        .batch_size(10)
        .build();
    consumer1.init().await.unwrap();

    let mut consumer2 = client_for_cg2_builder
        .consumer_group(consumer_group_2_name, STREAM_NAME, TOPIC_NAME)
        .unwrap()
        .auto_commit(AutoCommit::When(AutoCommitWhen::ConsumingAllMessages))
        .create_consumer_group_if_not_exists()
        .auto_join_consumer_group()
        .polling_strategy(PollingStrategy::next())
        .poll_interval(IggyDuration::from_str("1ms").unwrap())
        .batch_size(10)
        .build();
    consumer2.init().await.unwrap();

    (consumer1, consumer2)
}

async fn send_messages_and_poll_with_consumers(
    system_client: &IggyClient,
    consumer1: &mut IggyConsumer,
    consumer2: &mut IggyConsumer,
) {
    // 1. Send messages
    for i in 0..MESSAGES_COUNT {
        let message_payload = format!("message-{}", i);
        let message = IggyMessage::from_str(&message_payload).unwrap();
        system_client
            .send_messages(
                &Identifier::named(STREAM_NAME).unwrap(),
                &Identifier::named(TOPIC_NAME).unwrap(),
                &Partitioning::balanced(),
                &mut [message],
            )
            .await
            .unwrap();
    }

    let mut expected_payloads = (0..MESSAGES_COUNT)
        .map(|i| format!("message-{}", i))
        .collect::<Vec<_>>();
    expected_payloads.sort_unstable();

    // 2. Consumer 1 polls all messages
    let mut total_polled_cg1 = 0;
    let mut received_payloads_cg1 = Vec::new();
    while total_polled_cg1 < MESSAGES_COUNT {
        match consumer1.next().await {
            Some(Ok(received_message)) => {
                total_polled_cg1 += 1;
                received_payloads_cg1.push(
                    from_utf8(&received_message.message.payload)
                        .unwrap()
                        .to_string(),
                );
            }
            Some(Err(e)) => {
                panic!("Polling error for consumer 1: {:?}", e);
            }
            None => {
                // Stream ended before all messages were received by consumer 1.
                // The assertion below will catch this discrepancy.
                break;
            }
        }
    }
    assert_eq!(
        total_polled_cg1, MESSAGES_COUNT,
        "Consumer 1 did not poll all messages"
    );
    received_payloads_cg1.sort_unstable();
    assert_eq!(
        received_payloads_cg1, expected_payloads,
        "Message content mismatch for Consumer 1"
    );

    // 3. Consumer 2 polls all messages
    let mut total_polled_cg2 = 0;
    let mut received_payloads_cg2 = Vec::new();
    while total_polled_cg2 < MESSAGES_COUNT {
        match consumer2.next().await {
            Some(Ok(received_message)) => {
                total_polled_cg2 += 1;
                received_payloads_cg2.push(
                    from_utf8(&received_message.message.payload)
                        .unwrap()
                        .to_string(),
                );
            }
            Some(Err(e)) => {
                panic!("Polling error for consumer 2: {:?}", e);
            }
            None => {
                // Stream ended before all messages were received by consumer 2.
                break;
            }
        }
    }
    assert_eq!(
        total_polled_cg2, MESSAGES_COUNT,
        "Consumer 2 did not poll all messages"
    );
    received_payloads_cg2.sort_unstable();
    assert_eq!(
        received_payloads_cg2, expected_payloads,
        "Message content mismatch for Consumer 2"
    );
}

async fn get_consumer_group_details(client: &IggyClient, group_name: &str) -> ConsumerGroupDetails {
    client
        .get_consumer_group(
            &Identifier::named(STREAM_NAME).unwrap(),
            &Identifier::named(TOPIC_NAME).unwrap(),
            &Identifier::named(group_name).unwrap(),
        )
        .await
        .unwrap()
        .unwrap_or_else(|| panic!("Consumer group with NAME: {} not found.", group_name))
}

async fn cleanup(system_client: &IggyClient) {
    delete_user(system_client, USER_CG1_NAME).await;
    delete_user(system_client, USER_CG2_NAME).await;

    system_client
        .delete_stream(&Identifier::named(STREAM_NAME).unwrap())
        .await
        .unwrap();
}
