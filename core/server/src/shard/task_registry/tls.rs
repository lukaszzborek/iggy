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

use super::registry::TaskRegistry;
use std::cell::RefCell;
use std::rc::Rc;

thread_local! {
    static REGISTRY: RefCell<Option<Rc<TaskRegistry>>> = RefCell::new(None);
}

pub fn init_task_registry(shard_id: u16) {
    REGISTRY.with(|s| {
        *s.borrow_mut() = Some(Rc::new(TaskRegistry::new(shard_id)));
    });
}

pub fn task_registry() -> Rc<TaskRegistry> {
    REGISTRY.with(|s| {
        s.borrow()
            .as_ref()
            .expect("Task registry not initialized for this thread. Call init_registry() first.")
            .clone()
    })
}

pub fn is_registry_initialized() -> bool {
    REGISTRY.with(|s| s.borrow().is_some())
}

pub fn clear_registry() {
    REGISTRY.with(|s| {
        *s.borrow_mut() = None;
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::panic;

    #[test]
    fn test_registry_initialization() {
        clear_registry();
        assert!(!is_registry_initialized());

        let result = panic::catch_unwind(|| {
            task_registry();
        });
        assert!(result.is_err());

        init_task_registry(42);
        assert!(is_registry_initialized());

        clear_registry();
        assert!(!is_registry_initialized());
    }

    #[test]
    fn test_multiple_initializations() {
        clear_registry();
        init_task_registry(1);
        let _reg1 = task_registry();
        init_task_registry(2);
        let _reg2 = task_registry();
        clear_registry();
    }

    #[test]
    fn test_thread_locality() {
        use std::thread;

        clear_registry();
        init_task_registry(100);
        assert!(is_registry_initialized());

        let handle = thread::spawn(|| {
            assert!(!is_registry_initialized());
            init_task_registry(200);
            assert!(is_registry_initialized());
            let _ = task_registry();
        });

        handle.join().expect("Thread should complete successfully");
        assert!(is_registry_initialized());
        let _ = task_registry();
        clear_registry();
    }
}
