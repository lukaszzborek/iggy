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

pub mod buffer;
pub mod setup;

pub use setup::{TelemetryConfig, TelemetryContext};

use opentelemetry::KeyValue;
use opentelemetry::metrics::{Gauge, Histogram, Meter};
use std::sync::Arc;
use std::time::Instant;

/// Container for all OpenTelemetry metrics instruments
pub struct BenchMetrics {
    // Gauges for rates (per second) - the main metrics to watch
    pub messages_sent_per_sec: Gauge<f64>,
    pub messages_received_per_sec: Gauge<f64>,
    pub bytes_sent_mb_per_sec: Gauge<f64>,
    pub bytes_received_mb_per_sec: Gauge<f64>,
    pub batches_sent_per_sec: Gauge<f64>,

    // Histograms for latency distribution
    pub send_latency: Histogram<f64>,
    pub receive_latency: Histogram<f64>,
}

impl BenchMetrics {
    pub fn new(meter: &Meter) -> Self {
        Self {
            // Gauges for rates (per second)
            messages_sent_per_sec: meter
                .f64_gauge("iggy.bench.messages.sent.rate")
                .with_description("Messages sent per second")
                .with_unit("messages/sec")
                .build(),

            messages_received_per_sec: meter
                .f64_gauge("iggy.bench.messages.received.rate")
                .with_description("Messages received per second")
                .with_unit("messages/sec")
                .build(),

            bytes_sent_mb_per_sec: meter
                .f64_gauge("iggy.bench.throughput.sent")
                .with_description("Throughput sent in MB/s")
                .with_unit("MB/sec")
                .build(),

            bytes_received_mb_per_sec: meter
                .f64_gauge("iggy.bench.throughput.received")
                .with_description("Throughput received in MB/s")
                .with_unit("MB/sec")
                .build(),

            batches_sent_per_sec: meter
                .f64_gauge("iggy.bench.batches.sent.rate")
                .with_description("Batches sent per second")
                .with_unit("batches/sec")
                .build(),

            // Histograms (using milliseconds for better readability)
            send_latency: meter
                .f64_histogram("iggy.bench.latency.send")
                .with_description("Send operation latency in milliseconds")
                .with_unit("ms")
                .build(),

            receive_latency: meter
                .f64_histogram("iggy.bench.latency.receive")
                .with_description("Receive operation latency in milliseconds")
                .with_unit("ms")
                .build(),
        }
    }
}

/// Lightweight metrics handle for actors to record metrics
#[derive(Clone)]
pub struct MetricsHandle {
    // TODO: Will be used for direct metric recording in future optimizations
    #[allow(dead_code)]
    metrics: Arc<BenchMetrics>,
    buffer: Arc<buffer::MetricsBuffer>,
    actor_labels: Vec<KeyValue>,
}

impl MetricsHandle {
    pub fn new(
        metrics: Arc<BenchMetrics>,
        buffer: Arc<buffer::MetricsBuffer>,
        actor_type: &str,
        actor_id: u32,
        stream_id: u32,
    ) -> Self {
        let actor_labels = vec![
            KeyValue::new("actor_type", actor_type.to_string()),
            KeyValue::new("actor_id", i64::from(actor_id)),
            KeyValue::new("stream_id", i64::from(stream_id)),
        ];

        Self {
            metrics,
            buffer,
            actor_labels,
        }
    }

    /// Record a batch send operation
    pub fn record_batch_sent(&self, messages: u64, bytes: u64, latency_us: f64) {
        // Buffer the raw data for async processing
        // Convert microseconds to milliseconds for recording
        let latency_ms = latency_us / 1000.0;
        self.buffer.push(buffer::MetricEvent {
            timestamp: Instant::now(),
            event_type: buffer::EventType::BatchSent {
                messages,
                bytes,
                latency_ms,
            },
            labels: self.actor_labels.clone(),
        });
    }

    /// Record a batch receive operation
    pub fn record_batch_received(&self, messages: u64, bytes: u64, latency_us: f64) {
        // Convert microseconds to milliseconds for recording
        let latency_ms = latency_us / 1000.0;
        self.buffer.push(buffer::MetricEvent {
            timestamp: Instant::now(),
            event_type: buffer::EventType::BatchReceived {
                messages,
                bytes,
                latency_ms,
            },
            labels: self.actor_labels.clone(),
        });
    }

    /// Record throughput measurement
    pub fn record_throughput(&self, messages_per_sec: f64, mb_per_sec: f64) {
        self.buffer.push(buffer::MetricEvent {
            timestamp: Instant::now(),
            event_type: buffer::EventType::Throughput {
                messages_per_sec,
                mb_per_sec,
            },
            labels: self.actor_labels.clone(),
        });
    }
}
