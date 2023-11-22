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

use std::sync::atomic::{AtomicU64, Ordering};

use self::{
    block_cache::BlockCache, file_cache::FileHandleCache, manifest_cache::ManifestCache,
    table_meta_cache::TableMetaCache,
};
use crate::{config::CacheConfig, metric, metric::MetricUpdater};

pub mod block_cache;
pub mod file_cache;
pub mod manifest_cache;
pub mod table_meta_cache;

pub struct CStoreCacheHandler {
    pub block_cache: BlockCache,
    pub table_meta_cache: TableMetaCache,
    pub file_cache: FileHandleCache,
    pub manifest_cache: ManifestCache,

    last_block_cache_weight: AtomicU64,
    last_table_meta_cache_weight: AtomicU64,
}

impl CStoreCacheHandler {
    pub fn new(cache_config: &CacheConfig) -> Self {
        CStoreCacheHandler {
            block_cache: BlockCache::new(cache_config),
            table_meta_cache: TableMetaCache::new(cache_config),
            file_cache: FileHandleCache::new(cache_config),
            manifest_cache: ManifestCache::new(cache_config),
            last_block_cache_weight: AtomicU64::new(0),
            last_table_meta_cache_weight: AtomicU64::new(0),
        }
    }
}

impl MetricUpdater for CStoreCacheHandler {
    fn update(&self) {
        let current_block_weight = self.block_cache.weighted_size();
        let block_delta =
            current_block_weight - self.last_block_cache_weight.load(Ordering::SeqCst);
        self.last_block_cache_weight
            .store(current_block_weight, Ordering::SeqCst);
        metric::BLOCK_CACHE_MEMORY.increment(block_delta as f64);

        let current_table_meta_weight = self.table_meta_cache.weighted_size();
        let table_meta_delta =
            current_table_meta_weight - self.last_table_meta_cache_weight.load(Ordering::SeqCst);
        self.last_table_meta_cache_weight
            .store(current_table_meta_weight, Ordering::SeqCst);
        metric::TABLE_META_CACHE_MEMORY.increment(table_meta_delta as f64)
    }
}
