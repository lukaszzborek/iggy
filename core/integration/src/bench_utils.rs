// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use assert_cmd::prelude::CommandCargoExt;
use iggy::prelude::*;
use iggy_common::TransportProtocol;
use std::process::{Command, Stdio};
use uuid::Uuid;

const BENCH_FILES_PREFIX: &str = "bench_";
const MESSAGE_BATCHES: u64 = 100;
const MESSAGES_PER_BATCH: u64 = 100;
const DEFAULT_NUMBER_OF_STREAMS: u64 = 8;

pub fn run_bench_and_wait_for_finish(
    server_addr: &str,
    transport: &TransportProtocol,
    bench: &str,
    amount_of_data_to_process: IggyByteSize,
) {
    let mut command = Command::cargo_bin("iggy-bench").unwrap();

    // Calculate message size based on input
    let total_bytes_to_process_per_stream =
        amount_of_data_to_process.as_bytes_u64() / DEFAULT_NUMBER_OF_STREAMS;
    let messages_total: u64 = MESSAGES_PER_BATCH * MESSAGE_BATCHES;
    let message_size = total_bytes_to_process_per_stream / messages_total;

    command.args([
        "--messages-per-batch",
        &MESSAGES_PER_BATCH.to_string(),
        "--message-batches",
        &MESSAGE_BATCHES.to_string(),
        "--message-size",
        &message_size.to_string(),
        bench,
        &transport.to_string(),
        "--server-address",
        server_addr,
    ]);

    // Always pipe stdout/stderr to see real-time output
    command.stdout(Stdio::piped());
    command.stderr(Stdio::piped());

    let mut child = command.spawn().unwrap();

    // Capture output for displaying on failure
    let output = child.wait_with_output().unwrap();
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    // Always print output to help with debugging
    if !stdout.is_empty() {
        eprintln!("=== iggy-bench stdout ===\n{}", stdout);
    }
    if !stderr.is_empty() {
        eprintln!("=== iggy-bench stderr ===\n{}", stderr);
    }

    if !output.status.success() {
        panic!(
            "iggy-bench failed with exit code: {:?}\nSee output above for details",
            output.status.code()
        );
    }
}

pub fn get_random_path() -> String {
    format!("{}{}", BENCH_FILES_PREFIX, Uuid::now_v7().to_u128_le())
}
