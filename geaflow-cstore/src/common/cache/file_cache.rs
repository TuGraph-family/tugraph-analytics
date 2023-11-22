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

use std::sync::{Arc, Mutex, MutexGuard};

use crossbeam::queue::SegQueue;
use quick_cache::sync::Cache;

use crate::{config::CacheConfig, error::Error, file::file_handle::FileHandle, Result};

pub type FileRef = Arc<Mutex<Box<dyn FileHandle>>>;
pub type FileHandlePoolRef = Arc<SegQueue<FileRef>>;

#[derive(Default)]
pub struct FileRefOp {
    ref_op: Option<FileRef>,
}

impl FileRefOp {
    pub fn new(file: Box<dyn FileHandle>) -> Self {
        Self {
            ref_op: Some(Arc::new(Mutex::new(file))),
        }
    }

    pub fn new_with_ref(file_ref: FileRef) -> Self {
        Self {
            ref_op: Some(file_ref),
        }
    }

    pub fn get_ref(&self) -> MutexGuard<Box<dyn FileHandle>> {
        let t_ref = self.ref_op.as_ref().unwrap().lock().unwrap();
        t_ref
    }

    pub fn take(&mut self) -> Option<FileRef> {
        self.ref_op.take()
    }
}

#[derive(Default)]
pub struct FileHandlePoolRefOp {
    ref_op: Option<FileHandlePoolRef>,
}

impl FileHandlePoolRefOp {
    pub fn new(file_pool_ref: FileHandlePoolRef) -> Self {
        Self {
            ref_op: Some(file_pool_ref),
        }
    }

    pub fn get_ref(&self) -> &FileHandlePoolRef {
        self.ref_op.as_ref().unwrap()
    }

    pub fn pop(&mut self) -> Option<FileRef> {
        match self.ref_op.as_ref() {
            Some(queue) => queue.as_ref().pop(),
            None => None,
        }
    }

    pub fn push(&mut self, file_ref: FileRef) {
        if let Some(queue) = self.ref_op.as_ref() {
            queue.push(file_ref);
        }
    }

    pub fn size(&self) -> usize {
        match self.ref_op.as_ref() {
            Some(queue) => queue.len(),
            None => 0,
        }
    }
}

// TODO Need to add message sync or listener: compaction -> auto release
pub struct FileHandleCache {
    file_cache: Cache<u32, FileHandlePoolRef>,

    each_file_pool_capacity: usize,
}

impl FileHandleCache {
    pub fn new(cache_config: &CacheConfig) -> Self {
        let cache: Cache<u32, FileHandlePoolRef> = Cache::new(cache_config.file_cache_item_number);

        FileHandleCache {
            file_cache: cache,
            each_file_pool_capacity: cache_config.each_file_pool_capacity,
        }
    }

    pub fn get_with(&self, key: &u32) -> Result<FileHandlePoolRef> {
        self.file_cache
            .get_or_insert_with::<Error>(key, || Ok(Arc::new(SegQueue::new())))
    }

    pub fn remove(&self, key: &u32) -> bool {
        self.file_cache.remove(key)
    }

    // Estimate weight.
    pub fn weighted_size(&self) -> u64 {
        self.file_cache.weight()
    }

    pub fn capacity(&self) -> u64 {
        self.file_cache.capacity()
    }

    pub fn max_same_handle_num(&self) -> usize {
        self.each_file_pool_capacity
    }
}
