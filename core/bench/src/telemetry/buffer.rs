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

use opentelemetry::KeyValue;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tracing::warn;

/// Event types for metrics
#[derive(Clone, Debug)]
pub enum EventType {
    BatchSent {
        messages: u64,
        bytes: u64,
        latency_ms: f64, // Changed to milliseconds for better readability
    },
    BatchReceived {
        messages: u64,
        bytes: u64,
        latency_ms: f64, // Changed to milliseconds for better readability
    },
    Throughput {
        messages_per_sec: f64,
        mb_per_sec: f64,
    },
}

/// Single metric event to be processed
#[derive(Clone, Debug)]
pub struct MetricEvent {
    pub timestamp: Instant,
    pub event_type: EventType,
    pub labels: Vec<KeyValue>,
}

/// Lock-free metrics buffer using channels
/// This avoids blocking the benchmark threads
pub struct MetricsBuffer {
    sender: mpsc::UnboundedSender<MetricEvent>,
    dropped_count: Arc<AtomicU64>,
    buffer_size: Arc<AtomicUsize>,
}

impl MetricsBuffer {
    pub fn new(batch_size: usize, flush_interval: Duration) -> (Self, MetricsProcessor) {
        let (sender, receiver) = mpsc::unbounded_channel();

        let buffer = Self {
            sender,
            dropped_count: Arc::new(AtomicU64::new(0)),
            buffer_size: Arc::new(AtomicUsize::new(0)),
        };

        let processor = MetricsProcessor {
            receiver,
            batch_size,
            flush_interval,
            batch: Vec::with_capacity(batch_size),
            last_flush: Instant::now(),
            buffer_size: buffer.buffer_size.clone(),
            // Initialize rate calculation state
            last_rate_calc: Instant::now(),
            messages_sent_accumulator: 0,
            messages_received_accumulator: 0,
            bytes_sent_accumulator: 0,
            bytes_received_accumulator: 0,
            batches_sent_accumulator: 0,
        };

        (buffer, processor)
    }

    /// Push a metric event to the buffer (non-blocking)
    pub fn push(&self, event: MetricEvent) {
        // Try to send, but don't block if buffer is full
        match self.sender.send(event) {
            Ok(()) => {
                self.buffer_size.fetch_add(1, Ordering::Relaxed);
            }
            Err(_) => {
                // Channel is full, increment dropped counter
                self.dropped_count.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    pub fn dropped_events(&self) -> u64 {
        self.dropped_count.load(Ordering::Relaxed)
    }

    pub fn buffer_size(&self) -> usize {
        self.buffer_size.load(Ordering::Relaxed)
    }
}

/// Processes buffered metrics asynchronously
pub struct MetricsProcessor {
    receiver: mpsc::UnboundedReceiver<MetricEvent>,
    batch_size: usize,
    flush_interval: Duration,
    batch: Vec<MetricEvent>,
    last_flush: Instant,
    buffer_size: Arc<AtomicUsize>,
    // Rate calculation state
    last_rate_calc: Instant,
    messages_sent_accumulator: u64,
    messages_received_accumulator: u64,
    bytes_sent_accumulator: u64,
    bytes_received_accumulator: u64,
    batches_sent_accumulator: u64,
}

impl MetricsProcessor {
    /// Process metrics in a background task
    pub async fn run(mut self, metrics: Arc<super::BenchMetrics>) {
        loop {
            // Check if we should flush based on time
            let should_flush_time = self.last_flush.elapsed() >= self.flush_interval;

            // Try to receive events with a timeout
            let timeout = if should_flush_time {
                Duration::from_millis(1)
            } else {
                self.flush_interval - self.last_flush.elapsed()
            };

            match tokio::time::timeout(timeout, self.receiver.recv()).await {
                Ok(Some(event)) => {
                    self.batch.push(event);
                    // Decrement buffer size since we've received the event
                    self.buffer_size.fetch_sub(1, Ordering::Relaxed);

                    // Flush if batch is full
                    if self.batch.len() >= self.batch_size {
                        self.flush_batch(&metrics);
                    }
                }
                Ok(None) => {
                    // Channel closed, flush remaining and exit
                    if !self.batch.is_empty() {
                        self.flush_batch(&metrics);
                    }
                    break;
                }
                Err(_) => {
                    // Timeout - flush if we have data
                    if !self.batch.is_empty() && should_flush_time {
                        self.flush_batch(&metrics);
                    }
                }
            }
        }
    }

    fn flush_batch(&mut self, metrics: &super::BenchMetrics) {
        // Process all events in the batch and accumulate for rate calculation
        for event in self.batch.drain(..) {
            match event.event_type {
                EventType::BatchSent {
                    messages,
                    bytes,
                    latency_ms,
                } => {
                    // Record latency (now in milliseconds)
                    metrics.send_latency.record(latency_ms, &event.labels);

                    // Accumulate for rate calculation
                    self.messages_sent_accumulator += messages;
                    self.bytes_sent_accumulator += bytes;
                    self.batches_sent_accumulator += 1;
                }
                EventType::BatchReceived {
                    messages,
                    bytes,
                    latency_ms,
                } => {
                    // Record latency (now in milliseconds)
                    metrics.receive_latency.record(latency_ms, &event.labels);

                    // Accumulate for rate calculation
                    self.messages_received_accumulator += messages;
                    self.bytes_received_accumulator += bytes;
                }
                EventType::Throughput {
                    messages_per_sec: _,
                    mb_per_sec: _,
                } => {
                    // Throughput is now calculated from accumulated data
                }
            }
        }

        // Calculate and update rates
        let elapsed = self.last_rate_calc.elapsed().as_secs_f64();
        if elapsed > 0.01 {
            // Only update if at least 10ms have passed
            // Calculate rates per second
            let messages_sent_rate = self.messages_sent_accumulator as f64 / elapsed;
            let messages_received_rate = self.messages_received_accumulator as f64 / elapsed;
            let bytes_sent_mb_rate = (self.bytes_sent_accumulator as f64 / 1_048_576.0) / elapsed;
            let bytes_received_mb_rate =
                (self.bytes_received_accumulator as f64 / 1_048_576.0) / elapsed;
            let batches_sent_rate = self.batches_sent_accumulator as f64 / elapsed;

            // Update gauge metrics with current rates
            metrics
                .messages_sent_per_sec
                .record(messages_sent_rate, &[]);
            metrics
                .messages_received_per_sec
                .record(messages_received_rate, &[]);
            metrics
                .bytes_sent_mb_per_sec
                .record(bytes_sent_mb_rate, &[]);
            metrics
                .bytes_received_mb_per_sec
                .record(bytes_received_mb_rate, &[]);
            metrics.batches_sent_per_sec.record(batches_sent_rate, &[]);

            // Reset accumulators and timer
            self.messages_sent_accumulator = 0;
            self.messages_received_accumulator = 0;
            self.bytes_sent_accumulator = 0;
            self.bytes_received_accumulator = 0;
            self.batches_sent_accumulator = 0;
            self.last_rate_calc = Instant::now();
        }

        self.last_flush = Instant::now();
    }
}

/// Periodic reporter for buffer stats
pub struct BufferStatsReporter {
    buffer: Arc<MetricsBuffer>,
    interval: Duration,
}

impl BufferStatsReporter {
    pub const fn new(buffer: Arc<MetricsBuffer>, interval: Duration) -> Self {
        Self { buffer, interval }
    }

    pub async fn run(self) {
        let mut interval = tokio::time::interval(self.interval);
        loop {
            interval.tick().await;

            let dropped = self.buffer.dropped_events();
            if dropped > 0 {
                warn!(
                    "Metrics buffer dropped {} events (buffer size: {})",
                    dropped,
                    self.buffer.buffer_size()
                );
            }
        }
    }
}
