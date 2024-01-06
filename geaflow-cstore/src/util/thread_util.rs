// Copyright 2023 AntGroup CO., Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.

use std::ops::{Deref, DerefMut};

use dashmap::DashSet;
use itertools::Itertools;
use rayon::{
    prelude::{IntoParallelIterator, ParallelIterator},
    ThreadPoolBuilder,
};

use crate::log_util::debug;

#[derive(Debug)]
pub struct RayonThreadPool {
    pool: rayon::ThreadPool,
}

impl Deref for RayonThreadPool {
    type Target = rayon::ThreadPool;

    fn deref(&self) -> &Self::Target {
        &self.pool
    }
}

impl DerefMut for RayonThreadPool {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.pool
    }
}

impl Default for RayonThreadPool {
    fn default() -> Self {
        Self {
            pool: ThreadPoolBuilder::new().num_threads(1).build().unwrap(),
        }
    }
}

impl RayonThreadPool {
    pub fn new(num_threads: usize) -> Self {
        let pool = ThreadPoolBuilder::new()
            .num_threads(num_threads)
            .build()
            .unwrap();

        // Print the thread id.
        let thread_id_set: DashSet<usize> = DashSet::default();
        pool.install(|| {
            (0..2 * num_threads).into_par_iter().for_each(|_| {
                thread_id_set.insert(thread_id::get());
            });
        });

        debug!(
            "child thread id: {}",
            thread_id_set
                .into_iter()
                .map(|x| x.to_string())
                .collect_vec()
                .join(",")
        );

        RayonThreadPool { pool }
    }

    pub fn iter_execute<T>(
        &self,
        collection: impl IntoParallelIterator<Item = T> + Sync + Send,
        op: impl Fn(T) + Sync + Send,
    ) {
        self.pool.install(|| {
            collection.into_par_iter().for_each(|item| {
                op(item);
            });
        });

        self.pool.join(|| {}, || {});
    }
}

#[cfg(test)]
mod tests {
    use rustc_hash::FxHashSet;

    use super::RayonThreadPool;
    use crate::log_util::{self, info, LogLevel, LogType};

    #[test]
    fn test_rayon_thread_pool() {
        log_util::try_init(LogType::ConsoleAndFile, LogLevel::Debug, 0);

        let thread_pool = RayonThreadPool::new(10);

        let test_set: FxHashSet<u32> = [1, 2, 3, 4, 5].into_iter().collect();

        thread_pool.iter_execute(&test_set, |item| {
            info!(
                "Thread {:?} print test val {}",
                std::thread::current().id(),
                *item,
            );
        });
    }
}
