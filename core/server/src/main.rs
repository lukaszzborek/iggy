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

use clap::Parser;
use figlet_rs::FIGfont;
use server::args::Args;
use server::run_server::run_server;
use server::server_error::ServerError;

#[compio::main]
async fn main() -> Result<(), ServerError> {
    let standard_font = FIGfont::standard().unwrap();
    let figure = standard_font.convert("Iggy Server");
    println!("{}", figure.unwrap());

    if let Some(sha) = option_env!("VERGEN_GIT_SHA") {
        println!("Commit SHA: {sha}");
    }

    let args = Args::parse();
    // Run server with signal handler installed (binary mode)
    // This will block until shutdown signal is received
    let _handle = run_server(args, true).await?;
    Ok(())
}
