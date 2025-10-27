<<<<<<<< HEAD:core/server/src/shard/transmission/id.rs
/* Licensed to the Apache Software Foundation (ASF) under one
========
/**
 * Licensed to the Apache Software Foundation (ASF) under one
>>>>>>>> master:foreign/node/src/examples/utils.ts
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

<<<<<<<< HEAD:core/server/src/shard/transmission/id.rs
use std::ops::Deref;

// TODO: Maybe pad to cache line size?
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct ShardId {
    id: u16,
}

impl ShardId {
    pub fn new(id: u16) -> Self {
        Self { id }
    }

    pub fn id(&self) -> u16 {
        self.id
    }
}

impl Deref for ShardId {
    type Target = u16;

    fn deref(&self) -> &Self::Target {
        &self.id
    }
}
========

import { Client } from '../index.js';
import { getIggyAddress } from '../tcp.sm.utils.js';


export const getClient = () => {
  const [host, port] = getIggyAddress();
  const credentials = { username: 'iggy', password: 'iggy' };

  const opt = {
    transport: 'TCP' as const,
    options: { host, port },
    credentials
  };

  return new Client(opt);
};
>>>>>>>> master:foreign/node/src/examples/utils.ts
