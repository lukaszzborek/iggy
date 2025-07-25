# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

FROM rust:1.87-slim-bookworm

RUN apt-get update && \
    apt-get install -y \
    python3.11 \
    python3.11-dev \
    python3-pip \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Configure pip to allow system-wide installations in Docker
ENV PIP_BREAK_SYSTEM_PACKAGES=1

# Copy the entire workspace for dependencies
WORKDIR /workspace
COPY . .

# Set working directory for Python SDK
WORKDIR /workspace/foreign/python

# Install Python dependencies including BDD requirements
RUN pip3 install --no-cache-dir -r /workspace/bdd/python/requirements.txt

# Build and install the Iggy Python SDK
RUN maturin build --release --out dist && \
    pip3 install dist/*.whl

# Set up BDD test directory
WORKDIR /app
RUN mkdir -p /app/features
RUN mkdir -p /app/tests

# Copy BDD test files
RUN cp -r /workspace/bdd/python/tests/* /app/tests/

# Default command
CMD ["pytest", "tests/", "-v"]
