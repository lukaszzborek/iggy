use crate::{
    slab::traits_ext::{
        Borrow, Components, Delete, EntityComponentSystem, EntityMarker, IndexComponents, Insert, IntoComponents, 
    },
    streaming::{
        deduplication::message_deduplicator::MessageDeduplicator, partitions::{partition::ConsumerOffset, partition2}, segments,
        stats::stats::PartitionStats,
    },
};
use ahash::AHashMap;
use slab::Slab;
use std::{
    ops::Index,
    sync::{Arc, atomic::AtomicU64},
};

// TODO: This could be upper limit of partitions per topic, use that value to validate instead of whathever this thing is in `common` crate.
pub const PARTITIONS_CAPACITY: usize = 16384;
type Id = usize;

#[derive(Debug)]
pub struct Partitions {
    info: Slab<partition2::PartitionInfo>,
    stats: Slab<Arc<PartitionStats>>,
    segments: Slab<Vec<segments::Segment2>>,
    message_deduplicator: Slab<Option<MessageDeduplicator>>,
    offset: Slab<Arc<AtomicU64>>,

    consumer_offset: Slab<AHashMap<usize, ConsumerOffset>>,
    consumer_group_offset: Slab<AHashMap<usize, ConsumerOffset>>,
}

impl Insert<Id> for Partitions {
    type Item = Partition;

    fn insert(&mut self, item: Self::Item) -> Id {
        todo!();
    }
}

impl Delete<Id> for Partitions {
    type Item = Partition;

    fn delete(&mut self, id: Id) -> Self::Item {
        todo!()
    }
}

pub struct Partition {
    info: partition2::PartitionInfo,
    stats: Arc<PartitionStats>,
    message_deduplicator: Option<MessageDeduplicator>,
    offset: Arc<AtomicU64>,
}

impl EntityMarker for Partition {}

impl IntoComponents for Partition {
    type Components = (
        partition2::PartitionInfo,
        Arc<PartitionStats>,
        Option<MessageDeduplicator>,
        Arc<AtomicU64>,
    );

    fn into_components(self) -> Self::Components {
        (
            self.info,
            self.stats,
            self.message_deduplicator,
            self.offset,
        )
    }
}

pub struct PartitionRef<'a> {
    info: &'a Slab<partition2::PartitionInfo>,
    stats: &'a Slab<Arc<PartitionStats>>,
    message_deduplicator: &'a Slab<Option<MessageDeduplicator>>,
    offset: &'a Slab<Arc<AtomicU64>>,
}

impl<'a> From<&'a Partitions> for PartitionRef<'a> {
    fn from(value: &'a Partitions) -> Self {
        PartitionRef {
            info: &value.info,
            stats: &value.stats,
            message_deduplicator: &value.message_deduplicator,
            offset: &value.offset,
        }
    }
}

impl<'a> IntoComponents for PartitionRef<'a> {
    type Components = (
        &'a Slab<partition2::PartitionInfo>,
        &'a Slab<Arc<PartitionStats>>,
        &'a Slab<Option<MessageDeduplicator>>,
        &'a Slab<Arc<AtomicU64>>,
    );

    fn into_components(self) -> Self::Components {
        (
            self.info,
            self.stats,
            self.message_deduplicator,
            self.offset,
        )
    }
}

impl<'a> IndexComponents<Id> for PartitionRef<'a> {
    type Output = (
        &'a partition2::PartitionInfo,
        &'a Arc<PartitionStats>,
        &'a Option<MessageDeduplicator>,
        &'a Arc<AtomicU64>,
    );

    fn index(&self, index: Id) -> Self::Output {
        (
            &self.info[index],
            &self.stats[index],
            &self.message_deduplicator[index],
            &self.offset[index],
        )
    }
}

impl EntityComponentSystem<Id, Borrow> for Partitions {
    type Entity = Partition;
    type EntityRef<'a> = PartitionRef<'a>;

    fn with<O, F>(&self, f: F) -> O
    where
        F: for<'a> FnOnce(Self::EntityRef<'a>) -> O,
    {
        f(self.into())
    }

    async fn with_async<O, F>(&self, f: F) -> O
    where
        F: for<'a> AsyncFnOnce(Self::EntityRef<'a>) -> O,
    {
        f(self.into()).await
    }
}

impl Default for Partitions {
    fn default() -> Self {
        Self {
            info: Slab::with_capacity(PARTITIONS_CAPACITY),
            stats: Slab::with_capacity(PARTITIONS_CAPACITY),
            segments: Slab::with_capacity(PARTITIONS_CAPACITY),
            message_deduplicator: Slab::with_capacity(PARTITIONS_CAPACITY),
            offset: Slab::with_capacity(PARTITIONS_CAPACITY),
            consumer_offset: Slab::with_capacity(PARTITIONS_CAPACITY),
            consumer_group_offset: Slab::with_capacity(PARTITIONS_CAPACITY),
        }
    }
}

impl Partitions {
    pub fn with_stats<T>(&self, f: impl FnOnce(&Slab<Arc<PartitionStats>>) -> T) -> T {
        let stats = &self.stats;
        f(stats)
    }

    pub fn with_stats_mut<T>(&mut self, f: impl FnOnce(&mut Slab<Arc<PartitionStats>>) -> T) -> T {
        let mut stats = &mut self.stats;
        f(&mut stats)
    }

    pub fn with_segments(&self, partition_id: usize, f: impl FnOnce(&Vec<segments::Segment2>)) {
        let segments = &self.segments[partition_id];
        f(segments);
    }

    pub fn with_segment_id(
        &self,
        partition_id: usize,
        segment_id: usize,
        f: impl FnOnce(&segments::Segment2),
    ) {
        self.with_segments(partition_id, |segments| {
            // we could binary search for that segment technically, but this is fine for now.
            if let Some(segment) = segments.iter().find(|s| s.id == segment_id) {
                f(segment);
            }
        });
    }
}
