use crate::streaming::common::test_setup::TestSetup;
use bytes::BytesMut;
use iggy::prelude::*;
use server::configs::resource_quota::MemoryResourceQuota;
use server::configs::system::{CacheConfig, PartitionConfig, SegmentConfig, SystemConfig};
use server::streaming::partitions::partition::Partition;
use server::streaming::segments::IggyMessagesMut;
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
    [msgs_req_to_save(3), msgs_req_to_save(10),  msgs_req_to_save(1000)],
    [segment_size(500), segment_size(2000), segment_size(100000)],
    [index_cache_disabled(), index_cache_enabled()])]
#[tokio::test]
async fn test_get_messages_by_timestamp(
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
            .id(id)
            .payload(payload)
            .headers(headers)
            .build();
        all_messages.push(message);
    }

    let initial_timestamp = IggyTimestamp::now();
    let mut batch_timestamps = Vec::with_capacity(batch_sizes.len());
    let mut current_pos = 0;

    // Append all batches with timestamps
    for batch_size in batch_sizes {
        let batch =
            IggyMessagesMut::from(&all_messages[current_pos..current_pos + batch_size as usize]);
        assert_eq!(batch.count(), batch_size);
        partition.append_messages(batch, None).await.unwrap();
        batch_timestamps.push(IggyTimestamp::now());
        current_pos += batch_size as usize;
        sleep(std::time::Duration::from_millis(10));
    }

    let final_timestamp = IggyTimestamp::now();

    // Test 1: All messages from initial timestamp
    let all_loaded_messages = partition
        .get_messages_by_timestamp(initial_timestamp, total_messages)
        .await
        .unwrap();
    assert_eq!(
        all_loaded_messages.count(),
        total_messages,
        "Expected {} messages from initial timestamp, but got {}",
        total_messages,
        all_loaded_messages.count()
    );

    // Test 2: Get messages from middle timestamp (after 3rd batch)
    let middle_timestamp = batch_timestamps[2];
    // We expect to get all messages after the 3rd batch (this is where the test was failing)
    let expected_messages = batch_sizes[3] + batch_sizes[4]; // Only these two batches remain after batch 2
    let remaining_count = total_messages - (batch_sizes[0] + batch_sizes[1] + batch_sizes[2]);

    // Use a timestamp that's 50us earlier than the recorded batch timestamp
    // This ensures we catch the right batch boundary consistently
    let adjusted_timestamp = (middle_timestamp.as_micros() - 50).into();

    let middle_messages = partition
        .get_messages_by_timestamp(adjusted_timestamp, remaining_count)
        .await
        .unwrap();
    assert_eq!(
        middle_messages.count(),
        expected_messages,
        "Expected {} messages from middle timestamp, but got {}",
        expected_messages,
        middle_messages.count()
    );

    // Test 3: No messages from final timestamp
    let no_messages = partition
        .get_messages_by_timestamp(final_timestamp, 1)
        .await
        .unwrap();
    assert_eq!(
        no_messages.count(),
        0,
        "Expected no messages from final timestamp, but got {}",
        no_messages.count()
    );

    // Test 4: Small subset from initial timestamp
    let subset_size = 3;
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

    let subset_messages = subset_messages.into_messages_vec();

    println!("initial timestamp: {}", initial_timestamp.as_micros());

    for i in 0..subset_messages.len() {
        let loaded_message = &subset_messages[i];
        let original_message = &all_messages[i];
        assert_eq!(
            loaded_message.header.id, original_message.header.id,
            "Message ID mismatch at position {}: expected {}, got {}",
            i, original_message.header.id, loaded_message.header.id
        );
        assert_eq!(
            loaded_message.payload, original_message.payload,
            "Payload mismatch at position {}: expected {:?}, got {:?}",
            i, original_message.payload, loaded_message.payload
        );
        assert_eq!(
            loaded_message.user_headers, original_message.user_headers,
            "Headers mismatch at position {}: expected {:?}, got {:?}",
            i, original_message.user_headers, loaded_message.user_headers
        );
        assert!(
            loaded_message.header.timestamp >= initial_timestamp.as_micros(),
            "Message timestamp {} at position {} is less than initial timestamp {}",
            loaded_message.header.timestamp,
            i,
            initial_timestamp
        );
    }

    // Test 5: Messages spanning multiple batches (from middle of 2nd batch timestamp)
    let span_timestamp = batch_timestamps[1];
    let span_size = 8;
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

    // Make sure the timestamp we're using for comparison is accurate
    let span_timestamp_micros = span_timestamp.as_micros();
    println!("Span timestamp in microseconds: {}", span_timestamp_micros);

    for batch in spanning_messages.iter() {
        for msg in batch.iter() {
            let msg_timestamp = msg.msg_header().timestamp();
            assert!(
                msg_timestamp >= span_timestamp_micros,
                "Message timestamp {} should be >= span timestamp {}",
                msg_timestamp,
                span_timestamp_micros
            );
        }
    }
}
