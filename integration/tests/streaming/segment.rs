use crate::streaming::common::test_setup::TestSetup;
use bytes::Bytes;
use iggy::confirmation::Confirmation;
use iggy::prelude::*;
use server::configs::system::{PartitionConfig, SegmentConfig, SystemConfig};
use server::streaming::segments::*;
use std::str::FromStr;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use std::time::Duration;
use tokio::fs;
use tokio::time::sleep;

#[tokio::test]
async fn should_persist_segment() {
    let setup = TestSetup::init().await;
    let stream_id = 1;
    let topic_id = 2;
    let partition_id = 3;
    let start_offsets = get_start_offsets();
    for start_offset in start_offsets {
        let mut segment = Segment::create(
            stream_id,
            topic_id,
            partition_id,
            start_offset,
            setup.config.clone(),
            IggyExpiry::NeverExpire,
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
        );

        setup
            .create_partition_directory(stream_id, topic_id, partition_id)
            .await;
        segment.persist().await.unwrap();
        assert_persisted_segment(
            &setup
                .config
                .get_partition_path(stream_id, topic_id, partition_id),
            start_offset,
        )
        .await;
    }
}

#[tokio::test]
async fn should_load_existing_segment_from_disk() {
    let setup = TestSetup::init().await;
    let stream_id = 1;
    let topic_id = 2;
    let partition_id = 3;
    let start_offsets = get_start_offsets();
    for start_offset in start_offsets {
        let mut segment = Segment::create(
            stream_id,
            topic_id,
            partition_id,
            start_offset,
            setup.config.clone(),
            IggyExpiry::NeverExpire,
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
        );
        setup
            .create_partition_directory(stream_id, topic_id, partition_id)
            .await;
        segment.persist().await.unwrap();
        assert_persisted_segment(
            &setup
                .config
                .get_partition_path(stream_id, topic_id, partition_id),
            start_offset,
        )
        .await;

        let mut loaded_segment = Segment::create(
            stream_id,
            topic_id,
            partition_id,
            start_offset,
            setup.config.clone(),
            IggyExpiry::NeverExpire,
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
        );
        loaded_segment.load_from_disk().await.unwrap();
        let loaded_messages = loaded_segment.get_messages_by_offset(0, 10).await.unwrap();

        assert_eq!(loaded_segment.partition_id(), segment.partition_id());
        assert_eq!(loaded_segment.start_offset(), segment.start_offset());
        assert_eq!(loaded_segment.end_offset(), segment.end_offset());
        assert_eq!(
            loaded_segment.get_messages_size(),
            segment.get_messages_size()
        );
        assert_eq!(loaded_segment.is_closed(), segment.is_closed());
        assert_eq!(
            loaded_segment.messages_file_path(),
            segment.messages_file_path()
        );
        assert_eq!(loaded_segment.index_file_path(), segment.index_file_path());
        assert!(loaded_messages.is_empty());
    }
}

#[tokio::test]
async fn should_persist_and_load_segment_with_messages() {
    let setup = TestSetup::init().await;
    let stream_id = 1;
    let topic_id = 2;
    let partition_id = 3;
    let start_offset = 0;
    let mut segment = Segment::create(
        stream_id,
        topic_id,
        partition_id,
        start_offset,
        setup.config.clone(),
        IggyExpiry::NeverExpire,
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU64::new(0)),
    );

    setup
        .create_partition_directory(stream_id, topic_id, partition_id)
        .await;
    segment.persist().await.unwrap();
    assert_persisted_segment(
        &setup
            .config
            .get_partition_path(stream_id, topic_id, partition_id),
        start_offset,
    )
    .await;
    let messages_count = 10;
    let mut messages = Vec::new();
    let mut messages_size = 0;
    for i in 0..messages_count {
        let message = IggyMessage::with_id(i as u128, Bytes::from("test"));
        messages_size += message.get_size_bytes().as_bytes_u32();
        messages.push(message);
    }
    let batch = IggyMessagesBatchMut::from_messages(&messages, messages_size);

    segment.append_batch(0, batch).await.unwrap();
    segment.persist_messages(None).await.unwrap();
    let mut loaded_segment = Segment::create(
        stream_id,
        topic_id,
        partition_id,
        start_offset,
        setup.config.clone(),
        IggyExpiry::NeverExpire,
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU64::new(0)),
    );
    loaded_segment.load_from_disk().await.unwrap();
    let messages = loaded_segment
        .get_messages_by_offset(0, messages_count)
        .await
        .unwrap();
    assert_eq!(messages.count(), messages_count);
}

#[tokio::test]
async fn should_persist_and_load_segment_with_messages_with_nowait_confirmation() {
    let setup = TestSetup::init_with_config(SystemConfig {
        segment: SegmentConfig {
            server_confirmation: Confirmation::NoWait,
            ..Default::default()
        },
        ..Default::default()
    })
    .await;
    let stream_id = 1;
    let topic_id = 2;
    let partition_id = 3;
    let start_offset = 0;
    let mut segment = Segment::create(
        stream_id,
        topic_id,
        partition_id,
        start_offset,
        setup.config.clone(),
        IggyExpiry::NeverExpire,
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU64::new(0)),
    );

    setup
        .create_partition_directory(stream_id, topic_id, partition_id)
        .await;
    segment.persist().await.unwrap();
    assert_persisted_segment(
        &setup
            .config
            .get_partition_path(stream_id, topic_id, partition_id),
        start_offset,
    )
    .await;
    let messages_count = 10;
    let mut messages = Vec::new();
    let mut messages_size = 0;
    for i in 0..messages_count {
        let message = IggyMessage::with_id(i as u128, Bytes::from("test"));
        messages_size += message.get_size_bytes().as_bytes_u32();
        messages.push(message);
    }
    let batch = IggyMessagesBatchMut::from_messages(&messages, messages_size);
    segment.append_batch(0, batch).await.unwrap();
    segment
        .persist_messages(Some(Confirmation::NoWait))
        .await
        .unwrap();
    sleep(Duration::from_millis(200)).await;
    let mut loaded_segment = Segment::create(
        stream_id,
        topic_id,
        partition_id,
        start_offset,
        setup.config.clone(),
        IggyExpiry::NeverExpire,
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU64::new(0)),
    );
    loaded_segment.load_from_disk().await.unwrap();
    let messages = loaded_segment
        .get_messages_by_offset(0, messages_count)
        .await
        .unwrap();
    assert_eq!(messages.count(), messages_count);
}

#[tokio::test]
async fn given_all_expired_messages_segment_should_be_expired() {
    let config = SystemConfig {
        partition: PartitionConfig {
            enforce_fsync: true,
            ..Default::default()
        },
        segment: SegmentConfig {
            size: IggyByteSize::from_str("10B").unwrap(), // small size to force expiration
            ..Default::default()
        },
        ..Default::default()
    };
    let setup = TestSetup::init_with_config(config).await;
    let stream_id = 1;
    let topic_id = 2;
    let partition_id = 3;
    let start_offset = 0;
    let message_expiry_us = 100000;
    let message_expiry = message_expiry_us.into();
    let mut segment = Segment::create(
        stream_id,
        topic_id,
        partition_id,
        start_offset,
        setup.config.clone(),
        message_expiry,
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU64::new(0)),
    );

    setup
        .create_partition_directory(stream_id, topic_id, partition_id)
        .await;
    segment.persist().await.unwrap();
    assert_persisted_segment(
        &setup
            .config
            .get_partition_path(stream_id, topic_id, partition_id),
        start_offset,
    )
    .await;
    let messages_count = 10;
    let mut messages = Vec::new();
    let mut messages_size = 0;
    for i in 0..messages_count {
        let message = IggyMessage::with_id(i as u128, Bytes::from("test"));
        messages_size += message.get_size_bytes().as_bytes_u32();
        messages.push(message);
    }
    let batch = IggyMessagesBatchMut::from_messages(&messages, messages_size);
    segment.append_batch(0, batch).await.unwrap();
    segment.persist_messages(None).await.unwrap();

    let is_expired = segment.is_expired(IggyTimestamp::now()).await;
    assert!(is_expired);
}

#[tokio::test]
async fn given_at_least_one_not_expired_message_segment_should_not_be_expired() {
    let config = SystemConfig {
        partition: PartitionConfig {
            enforce_fsync: true,
            ..Default::default()
        },
        segment: SegmentConfig {
            size: IggyByteSize::from_str("10B").unwrap(), // small size to force expiration
            ..Default::default()
        },
        ..Default::default()
    };
    let setup = TestSetup::init_with_config(config).await;
    let stream_id = 1;
    let topic_id = 2;
    let partition_id = 3;
    let start_offset = 0;
    let message_expiry_us = 50000;
    let message_expiry = message_expiry_us.into();
    let mut segment = Segment::create(
        stream_id,
        topic_id,
        partition_id,
        start_offset,
        setup.config.clone(),
        message_expiry,
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU64::new(0)),
    );

    setup
        .create_partition_directory(stream_id, topic_id, partition_id)
        .await;
    segment.persist().await.unwrap();
    assert_persisted_segment(
        &setup
            .config
            .get_partition_path(stream_id, topic_id, partition_id),
        start_offset,
    )
    .await;

    let nothing_expired_ts = IggyTimestamp::now();

    let first_message = vec![IggyMessage::create(Bytes::from("expired"))];
    let first_message_size = first_message[0].get_size_bytes().as_bytes_u32();
    let first_batch = IggyMessagesBatchMut::from_messages(&first_message, first_message_size);
    segment.append_batch(0, first_batch).await.unwrap();

    sleep(Duration::from_micros(message_expiry_us / 2)).await;
    let first_message_expired_ts = IggyTimestamp::now();

    let second_message = vec![IggyMessage::create(Bytes::from("not-expired"))];
    let second_message_size = second_message[0].get_size_bytes().as_bytes_u32();
    let second_batch = IggyMessagesBatchMut::from_messages(&second_message, second_message_size);
    segment.append_batch(1, second_batch).await.unwrap();

    let second_message_expired_ts =
        IggyTimestamp::now() + IggyDuration::from(message_expiry_us * 2);

    segment.persist_messages(None).await.unwrap();

    assert!(
        !segment.is_expired(nothing_expired_ts).await,
        "Segment should not be expired for nothing expired timestamp"
    );
    assert!(
        !segment.is_expired(first_message_expired_ts).await,
        "Segment should not be expired for first message expired timestamp"
    );
    assert!(
        segment.is_expired(second_message_expired_ts).await,
        "Segment should be expired for second message expired timestamp"
    );
}

async fn assert_persisted_segment(partition_path: &str, start_offset: u64) {
    let segment_path = format!("{}/{:0>20}", partition_path, start_offset);
    let messages_file_path = format!("{}.{}", segment_path, LOG_EXTENSION);
    let index_path = format!("{}.{}", segment_path, INDEX_EXTENSION);
    assert!(fs::metadata(&messages_file_path).await.is_ok());
    assert!(fs::metadata(&index_path).await.is_ok());
}

fn get_start_offsets() -> Vec<u64> {
    vec![
        0, 1, 2, 9, 10, 99, 100, 110, 200, 1000, 1234, 12345, 100000, 9999999,
    ]
}
