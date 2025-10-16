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

//! External server runner for sync mode tests
//! Starts iggy-server as a separate process using pre-built binary

use assert_cmd::prelude::CommandCargoExt;
use std::net::TcpStream;
use std::process::{Child, Command, Stdio};
use std::thread::sleep;
use std::time::Duration;

const SLEEP_INTERVAL_MS: u64 = 100;
const MAX_WAIT_S: u64 = 60;

pub struct ExternalServer {
    child: Option<Child>,
    tcp_addr: String,
    system_path: String,
}

impl ExternalServer {
    pub fn new() -> Self {
        let random_id: u128 = rand::random();
        let system_path = format!("local_data_external_{}", random_id);

        Self {
            child: None,
            tcp_addr: String::new(),
            system_path,
        }
    }

    pub fn start(&mut self) {
        println!("Starting external iggy-server for sync tests...");

        let child = Command::cargo_bin("iggy-server")
            .expect("Failed to find iggy-server binary. Run: cargo build -p server")
            .env("IGGY_SYSTEM_PATH", &self.system_path)
            .env("IGGY_TCP_ADDRESS", "127.0.0.1:8090")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("Failed to start iggy-server");

        let addr = "127.0.0.1:8090";

        self.child = Some(child);

        let start = std::time::Instant::now();
        loop {
            if TcpStream::connect(addr).is_ok() {
                println!("External server is ready on {}", addr);
                break;
            }

            if start.elapsed().as_secs() > MAX_WAIT_S {
                panic!(
                    "External server failed to start within {} seconds.",
                    MAX_WAIT_S
                );
            }

            print!(".");
            std::io::Write::flush(&mut std::io::stdout()).ok();
            sleep(Duration::from_millis(SLEEP_INTERVAL_MS));
        }
    }

    pub fn stop(&mut self) {
        if let Some(mut child) = self.child.take() {
            #[cfg(unix)]
            unsafe {
                use libc::{SIGTERM, kill};
                kill(child.id() as libc::pid_t, SIGTERM);
            }

            #[cfg(not(unix))]
            child.kill().ok();

            child.wait().ok();
        }

        if std::path::Path::new(&self.system_path).exists() {
            std::fs::remove_dir_all(&self.system_path).ok();
        }
    }

    pub fn tcp_addr(&self) -> &str {
        &self.tcp_addr
    }

    pub fn is_started(&self) -> bool {
        self.child.is_some()
    }

    pub fn system_path(&self) -> &str {
        &self.system_path
    }
}

impl Drop for ExternalServer {
    fn drop(&mut self) {
        self.stop();
    }
}

impl Default for ExternalServer {
    fn default() -> Self {
        Self::new()
    }
}
