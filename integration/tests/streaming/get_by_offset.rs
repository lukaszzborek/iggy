use crate::streaming::common::test_setup::TestSetup;
use bytes::BytesMut;
use iggy::prelude::*;
use server::configs::system::{PartitionConfig, SegmentConfig, SystemConfig};
use server::streaming::partitions::partition::Partition;
use server::streaming::segments::IggyMessagesBatchMut;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::atomic::{AtomicU32, AtomicU64};
use std::sync::Arc;
use test_case::test_matrix;

/*
 * Below helper functions are here only to make test function name more readable.
 */

fn msg_size(size: u64) -> IggyByteSize {
    IggyByteSize::from_str(&format!("{}b", size)).unwrap()
}

fn segment_size(size: u64) -> IggyByteSize {
    IggyByteSize::from_str(&format!("{}b", size)).unwrap()
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

#[test_matrix(
    [msg_size(50), msg_size(1000), msg_size(20000)],
    [msgs_req_to_save(10), msgs_req_to_save(24), msgs_req_to_save(1000)],
    [segment_size(500), segment_size(2000), segment_size(100000)],
    [index_cache_disabled(), index_cache_enabled()])]
#[tokio::test]
async fn test_get_messages_by_offset(
    message_size: IggyByteSize,
    messages_required_to_save: u32,
    segment_size: IggyByteSize,
    index_cache_enabled: bool,
) {
    println!(
        "Running test with messages_required_to_save: {}, segment_size: {}, message_size: {}, cache_indexes: {}",
        messages_required_to_save,
        segment_size,
        message_size,
        index_cache_enabled
    );

    let setup = TestSetup::init().await;
    let stream_id = 1;
    let topic_id = 1;
    let partition_id = 1;

    // Define batch sizes for 5 appends
    let batch_sizes = [3, 4, 5, 6, 7];
    let total_messages: u32 = batch_sizes.iter().sum();

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

    let mut all_messages = Vec::with_capacity(total_messages as usize);
    for i in 1..=total_messages {
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
            .with_id(id)
            .with_payload(payload)
            .with_user_headers_map(headers)
            .build();
        all_messages.push(message);
    }

    // Keep track of offsets after each batch
    let mut batch_offsets = Vec::with_capacity(batch_sizes.len());
    let mut current_pos = 0;

    // Append all batches
    for batch_len in batch_sizes {
        let messages_slice_to_append = &all_messages[current_pos..current_pos + batch_len as usize];

        let messages_size = messages_slice_to_append
            .iter()
            .map(|m| m.get_size_bytes().as_bytes_u64() as u32)
            .sum();

        let batch = IggyMessagesBatchMut::from_messages(messages_slice_to_append, messages_size);
        assert_eq!(batch.count(), batch_len);
        partition.append_messages(batch, None).await.unwrap();

        batch_offsets.push(partition.current_offset);
        current_pos += batch_len as usize;
    }

    // Test 1: All messages from start
    let all_loaded_messages = partition
        .get_messages_by_offset(0, total_messages)
        .await
        .unwrap();
    assert_eq!(
        all_loaded_messages.count(),
        total_messages,
        "Expected {} messages from start, but got {}",
        total_messages,
        all_loaded_messages.count()
    );

    // Test 2: Get messages from middle (after 3rd batch)
    let middle_offset = batch_offsets[2];
    let remaining_messages = total_messages - (batch_sizes[0] + batch_sizes[1] + batch_sizes[2]);
    let middle_messages = partition
        .get_messages_by_offset(middle_offset + 1, remaining_messages)
        .await
        .unwrap();
    assert_eq!(
        middle_messages.count(),
        remaining_messages,
        "Expected {} messages from middle offset, but got {}",
        remaining_messages,
        middle_messages.count()
    );

    // Test 3: No messages beyond final offset
    let final_offset = batch_offsets.last().unwrap();
    let no_messages = partition
        .get_messages_by_offset(final_offset + 1, 1)
        .await
        .unwrap();
    assert_eq!(
        no_messages.count(),
        0,
        "Expected no messages beyond final offset, but got {}",
        no_messages.count()
    );

    // Test 4: Small subset from start
    let subset_size = 3;
    let subset_messages = partition
        .get_messages_by_offset(0, subset_size)
        .await
        .unwrap();
    assert_eq!(
        subset_messages.count(),
        subset_size,
        "Expected {} messages in subset from start, but got {}",
        subset_size,
        subset_messages.count()
    );

    // Test 5: Messages spanning multiple batches
    let span_offset = batch_offsets[1] + 1; // Start from middle of 2nd batch
    let span_size = 8; // Should span across 2nd, 3rd, and into 4th batch
    let batches = partition
        .get_messages_by_offset(span_offset, span_size)
        .await
        .unwrap();
    assert_eq!(
        batches.count(),
        span_size,
        "Expected {} messages spanning multiple batches, but got {}",
        span_size,
        batches.count()
    );

    // Test 6: Validate message content and ordering
    let mut i = 0;
    for batch in batches.iter() {
        for msg in batch.iter() {
            println!(
                "Message at position {}, offset: {}",
                i,
                msg.header().offset()
            );
            let expected_offset = span_offset + i as u64;
            assert!(
                msg.header().offset() >= expected_offset,
                "Message offset {} at position {} should be >= expected offset {}",
                msg.header().offset(),
                i,
                expected_offset
            );

            let original_offset = msg.header().offset() as usize;
            let original_message = &all_messages[original_offset];

            let loaded_id = msg.header().id();
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
            let original_headers =
                HashMap::from_bytes(original_message.user_headers.as_ref().unwrap().clone())
                    .unwrap();
            assert_eq!(
                loaded_headers,
                original_headers,
                "Headers mismatch at offset {}",
                msg.header().offset(),
            );
            i += 1;
        }
    }
}
