use crate::streaming::common::test_setup::TestSetup;
use bytes::BytesMut;
use iggy::confirmation::Confirmation;
use iggy::prelude::*;
use server::configs::system::{PartitionConfig, SegmentConfig, SystemConfig};
use server::streaming::partitions::partition::Partition;
use server::streaming::segments::IggyMessagesBatchMut;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::atomic::{AtomicU32, AtomicU64};
use std::sync::Arc;
use std::thread::sleep;
use test_case::test_matrix;

/*
 * Below helper functions are here only to make test function name more readable.
 */

fn msg_size(size: u64) -> IggyByteSize {
    IggyByteSize::from_str(&format!("{}B", size)).unwrap()
}

fn segment_size(size: u64) -> IggyByteSize {
    IggyByteSize::from_str(&format!("{}B", size)).unwrap()
}

fn msgs_req_to_save(count: u32) -> u32 {
    count
}

fn index_cache_enabled() -> bool {
    true
}

fn index_cache_disabled() -> bool {
    false
}

fn small_batches() -> Vec<u32> {
    vec![3, 4, 5, 6, 7]
}

fn medium_batches() -> Vec<u32> {
    vec![10, 20, 30, 40]
}

fn large_batches() -> Vec<u32> {
    vec![100, 200, 300, 400]
}

fn very_large_batches() -> Vec<u32> {
    vec![500, 1000, 1500, 1000]
}

#[test_matrix(
    [msg_size(50), msg_size(1000), msg_size(20000)],
    [small_batches(), medium_batches(), large_batches(), very_large_batches()],
    [msgs_req_to_save(10), msgs_req_to_save(24), msgs_req_to_save(1000), msgs_req_to_save(4000)],
    [segment_size(500), segment_size(2000), segment_size(100000), segment_size(10000000)],
    [index_cache_disabled(), index_cache_enabled()])]
#[tokio::test]
async fn test_get_messages_by_timestamp(
    message_size: IggyByteSize,
    batch_sizes: Vec<u32>,
    messages_required_to_save: u32,
    segment_size: IggyByteSize,
    index_cache_enabled: bool,
) {
    println!(
        "Running test with message_size: {}, batches: {:?}, messages_required_to_save: {}, segment_size: {}, cache_indexes: {}",
        message_size,
        batch_sizes,
        messages_required_to_save,
        segment_size,
        index_cache_enabled
    );

    let setup = TestSetup::init().await;
    let stream_id = 1;
    let topic_id = 1;
    let partition_id = 1;

    let total_messages_count = batch_sizes.iter().sum();

    let config = Arc::new(SystemConfig {
        path: setup.config.path.to_string(),
        partition: PartitionConfig {
            messages_required_to_save,
            enforce_fsync: true,
            ..Default::default()
        },
        segment: SegmentConfig {
            cache_indexes: index_cache_enabled,
            size: segment_size,
            ..Default::default()
        },
        ..Default::default()
    });

    let mut partition = Partition::create(
        stream_id,
        topic_id,
        partition_id,
        true,
        config.clone(),
        setup.storage.clone(),
        IggyExpiry::NeverExpire,
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU32::new(0)),
        IggyTimestamp::now(),
    )
    .await;

    setup.create_partitions_directory(stream_id, topic_id).await;
    partition.persist().await.unwrap();

    let mut all_messages = Vec::with_capacity(total_messages_count as usize);

    // Generate all messages as defined in the test matrix
    for i in 1..=total_messages_count {
        let id = i as u128;
        let beginning_of_payload = format!("message {}", i);
        let mut payload = BytesMut::new();
        payload.extend_from_slice(beginning_of_payload.as_bytes());
        payload.resize(message_size.as_bytes_usize(), 0xD);
        let payload = payload.freeze();

        let mut headers = HashMap::new();
        headers.insert(
            HeaderKey::new("key_1").unwrap(),
            HeaderValue::from_str("Value 1").unwrap(),
        );
        headers.insert(
            HeaderKey::new("key 2").unwrap(),
            HeaderValue::from_bool(true).unwrap(),
        );
        headers.insert(
            HeaderKey::new("key-3").unwrap(),
            HeaderValue::from_uint64(123456).unwrap(),
        );

        let message = IggyMessage::builder()
            .id(id)
            .payload(payload)
            .headers(headers)
            .build()
            .expect("Failed to create message with valid payload and headers");

        all_messages.push(message);
    }

    // Timestamp tracking for messages
    let initial_timestamp = IggyTimestamp::now();
    let mut batch_timestamps = Vec::with_capacity(batch_sizes.len());
    let mut current_pos = 0;

    // Append all batches as defined in the test matrix with separate timestamps
    for (batch_idx, &batch_len) in batch_sizes.iter().enumerate() {
        // Add a small delay between batches to ensure distinct timestamps
        sleep(std::time::Duration::from_millis(2));

        // If we've generated too many messages, skip the rest
        if current_pos + batch_len as usize > all_messages.len() {
            break;
        }

        println!(
            "Appending batch {}/{} with {} messages",
            batch_idx + 1,
            batch_sizes.len(),
            batch_len
        );

        let batch_end_pos = current_pos + batch_len as usize;
        let messages_slice_to_append = &all_messages[current_pos..batch_end_pos];

        let messages_size = messages_slice_to_append
            .iter()
            .map(|m| m.get_size_bytes().as_bytes_u64() as u32)
            .sum();

        let batch = IggyMessagesBatchMut::from_messages(messages_slice_to_append, messages_size);
        assert_eq!(batch.count(), batch_len);
        partition
            .append_messages(batch, Some(Confirmation::Wait))
            .await
            .unwrap();

        // Capture the timestamp of this batch
        batch_timestamps.push(IggyTimestamp::now());
        current_pos += batch_len as usize;

        // Add a small delay between batches to ensure distinct timestamps
        sleep(std::time::Duration::from_millis(2));
    }

    let final_timestamp = IggyTimestamp::now();

    // Use the exact total messages count from the test matrix
    let total_sent_messages = total_messages_count;

    // Test 1: All messages from initial timestamp
    let all_loaded_messages = partition
        .get_messages_by_timestamp(initial_timestamp, total_sent_messages)
        .await
        .unwrap();
    assert_eq!(
        all_loaded_messages.count(),
        total_sent_messages,
        "Expected {} messages from initial timestamp, but got {}",
        total_sent_messages,
        all_loaded_messages.count()
    );

    // Test 2: Get messages from middle timestamp (after 3rd batch)
    if batch_timestamps.len() >= 3 {
        // Use a timestamp that's just before the 3rd batch's timestamp to ensure we get messages
        // from that batch onwards
        let middle_timestamp = IggyTimestamp::from(batch_timestamps[2].as_micros() + 1000);

        // Calculate how many messages should be in batches after the 3rd
        let prior_batches_sum: u32 = batch_sizes[..3].iter().sum();
        let remaining_messages = total_sent_messages - prior_batches_sum;

        let middle_messages = partition
            .get_messages_by_timestamp(middle_timestamp, remaining_messages)
            .await
            .unwrap();

        assert_eq!(
            middle_messages.count(),
            remaining_messages,
            "Expected {} messages from middle timestamp, but got {}",
            remaining_messages,
            middle_messages.count()
        );
    }

    // Test 3: No messages after final timestamp
    let no_messages = partition
        .get_messages_by_timestamp(final_timestamp, 1)
        .await
        .unwrap();
    assert_eq!(
        no_messages.count(),
        0,
        "Expected no messages after final timestamp, but got {}",
        no_messages.count()
    );

    // Test 4: Small subset from initial timestamp
    let subset_size = std::cmp::min(3, total_sent_messages);
    let subset_messages = partition
        .get_messages_by_timestamp(initial_timestamp, subset_size)
        .await
        .unwrap();
    assert_eq!(
        subset_messages.count(),
        subset_size,
        "Expected {} messages in subset from initial timestamp, but got {}",
        subset_size,
        subset_messages.count()
    );

    // Test 5: Messages spanning multiple batches by timestamp
    if batch_timestamps.len() >= 4 {
        // Use a timestamp that's just before the 2nd batch's timestamp
        let span_timestamp = IggyTimestamp::from(batch_timestamps[1].as_micros() + 1000);
        let span_size = 8; // Should span across multiple batches

        let spanning_messages = partition
            .get_messages_by_timestamp(span_timestamp, span_size)
            .await
            .unwrap();

        assert_eq!(
            spanning_messages.count(),
            span_size,
            "Expected {} messages spanning multiple batches, but got {}",
            span_size,
            spanning_messages.count()
        );

        // Verify that all messages have timestamps >= our reference timestamp
        let span_timestamp_micros = span_timestamp.as_micros();

        // Test 6: Validate message content and ordering
        for batch in spanning_messages.iter() {
            for msg in batch.iter() {
                let msg_timestamp = msg.header().timestamp();
                assert!(
                    msg_timestamp >= span_timestamp_micros,
                    "Message timestamp {} should be >= span timestamp {}",
                    msg_timestamp,
                    span_timestamp_micros
                );

                // Verify message content
                let loaded_id = msg.header().id();
                let original_offset = msg.header().offset() as usize;

                if original_offset < all_messages.len() {
                    let original_message = &all_messages[original_offset];
                    let original_id = original_message.header.id;

                    assert_eq!(
                        loaded_id,
                        original_id,
                        "Message ID mismatch at offset {}",
                        msg.header().offset(),
                    );

                    let loaded_payload = msg.payload();
                    let original_payload = &original_message.payload;
                    assert_eq!(
                        loaded_payload,
                        original_payload,
                        "Payload mismatch at offset {}",
                        msg.header().offset(),
                    );

                    let loaded_headers = msg.user_headers_map().unwrap().unwrap();
                    let original_headers = HashMap::from_bytes(
                        original_message.user_headers.as_ref().unwrap().clone(),
                    )
                    .unwrap();
                    assert_eq!(
                        loaded_headers,
                        original_headers,
                        "Headers mismatch at offset {}",
                        msg.header().offset(),
                    );
                }
            }
        }
    }

    // // Add sequential read test by timestamp
    // println!(
    //     "Verifying sequential time-based reads, expecting {} messages",
    //     total_sent_messages
    // );

    // // For timestamp-based sequential reads, we'll use timestamps from batch_timestamps
    // // array to retrieve messages in batches (which are part of returned batch_set)
    // if !batch_timestamps.is_empty() {
    //     let chunk_size = 500;
    //     let mut verified_count = 0;

    //     let mut current_timestamp = initial_timestamp;

    //     while verified_count < total_sent_messages {
    //         let msgs_to_read = std::cmp::min(chunk_size, total_sent_messages - verified_count);

    //         let batch_set = partition
    //             .get_messages_by_timestamp(current_timestamp, msgs_to_read)
    //             .await
    //             .unwrap();

    //         if batch_set.count() == 0 {
    //             println!(
    //                 "No messages found for timestamp {}, expected {}",
    //                 current_timestamp.as_micros(),
    //                 msgs_to_read
    //             );
    //             // We've read all messages, or there's a gap in the timestamps
    //             break;
    //         }

    //         verified_count += batch_set.count();

    //         println!(
    //             "Read chunk from timestamp {} with size {}, verified count: {}, first offset {}, last offset {}",
    //             current_timestamp.as_micros(),
    //             batch_set.count(),
    //             verified_count,
    //             batch_set
    //                 .iter()
    //                 .next()
    //                 .unwrap()
    //                 .iter()
    //                 .next()
    //                 .unwrap()
    //                 .header()
    //                 .offset(),
    //             batch_set
    //                 .iter()
    //                 .last()
    //                 .unwrap()
    //                 .iter()
    //                 .last()
    //                 .unwrap()
    //                 .header()
    //                 .offset()
    //         );

    //         // Get the last message timestamp from the batch set and use it for the next query
    //         // Add a small offset to avoid getting the same message again
    //         if let Some(batch) = batch_set.iter().last() {
    //             if let Some(msg) = batch.iter().last() {
    //                 current_timestamp = IggyTimestamp::from(msg.header().timestamp() + 1);
    //             }
    //         }
    //     }

    //     assert_eq!(
    //         verified_count, total_sent_messages,
    //         "Sequential timestamp batch set reads didn't cover all messages"
    //     );
    // }
}
