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

use std::ops::{Deref, DerefMut};

pub mod mutex;
#[cfg(not(feature = "sync"))]
pub mod semaphore;

cfg_if::cfg_if! {
    if #[cfg(feature = "std_sync_lock")] {
        mod std_sync_lock;
        pub type IggySharedMut<T> = std_sync_lock::IggyStdSyncRwLock<T>;
    } else if #[cfg(feature = "tokio_lock")] {
        mod tokio_lock;
        pub type IggySharedMut<T> = tokio_lock::IggyTokioRwLock<T>;
    } else if #[cfg(feature = "fast_async_lock")] {
        mod fast_async_lock;
        pub type IggySharedMut<T> = fast_async_lock::IggyFastAsyncRwLock<T>;
    }
}

#[maybe_async::maybe_async(AFIT)]
#[cfg_attr(not(feature = "sync"), allow(async_fn_in_trait))]
pub trait IggySharedMutFn<T>: Sync {
    type ReadGuard<'a>: Deref<Target = T>
    where
        T: 'a,
        Self: 'a;
    type WriteGuard<'a>: DerefMut<Target = T>
    where
        T: 'a,
        Self: 'a;

    fn new(data: T) -> Self
    where
        Self: Sized;

    async fn read<'a>(&'a self) -> Self::ReadGuard<'a>
    where
        T: 'a;

    async fn write<'a>(&'a self) -> Self::WriteGuard<'a>
    where
        T: 'a;
}
