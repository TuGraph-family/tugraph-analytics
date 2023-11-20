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

use std::{mem, num::NonZeroU32, sync::Arc};

use prost::Message;
use quick_cache::{sync::Cache, Weighter};
use rustc_hash::FxHashMap;

use super::file_cache::FileRefOp;
use crate::{
    config::CacheConfig,
    convert_usize_to_u32,
    error::Error,
    gen::meta::{BlockMeta, TableMeta},
    get_u32_from_bytes,
    table::get_upper_bound,
    Result, BLOCK_RECORD_LENGTH_SIZE_U32, BLOCK_RECORD_NUM_OFFSET_USIZE, BLOCK_RECORD_NUM_SIZE_U64,
    SIZE_OF_U32,
};

#[derive(Debug)]
pub struct TableMetaBlock {
    // TableMeta of the table.
    pub table_meta: TableMeta,
    // Map of BlockIndex -> BlockMeta.
    pub meta_list_map: FxHashMap<u32, BlockMeta>,
}

const BLOCK_META_SIZE: usize = mem::size_of::<BlockMeta>();

impl TableMetaBlock {
    pub fn get_estimated_weight(&self) -> u32 {
        let table_meta_size: usize =
            SIZE_OF_U32 * 2 + self.table_meta.block_meta.len() * BLOCK_META_SIZE;
        let block_meta_map_size: usize = self.meta_list_map.len() * (BLOCK_META_SIZE + SIZE_OF_U32);

        convert_usize_to_u32!(table_meta_size + block_meta_map_size)
    }
}

#[derive(Clone)]
pub struct TableMetaWeigher;

impl Weighter<u32, (), Arc<TableMetaBlock>> for TableMetaWeigher {
    fn weight(&self, _key: &u32, _qey: &(), val: &Arc<TableMetaBlock>) -> NonZeroU32 {
        NonZeroU32::new(val.get_estimated_weight()).unwrap()
    }
}

pub struct TableMetaCache {
    table_meta_cache: Cache<u32, Arc<TableMetaBlock>, TableMetaWeigher>,
}

impl TableMetaCache {
    pub fn new(cache_config: &CacheConfig) -> Self {
        let cache: Cache<u32, Arc<TableMetaBlock>, TableMetaWeigher> = Cache::with_weighter(
            cache_config.table_meta_cache_item_number,
            cache_config.table_meta_cache_capacity,
            TableMetaWeigher,
        );

        TableMetaCache {
            table_meta_cache: cache,
        }
    }

    pub fn get_with(&self, key: &u32, file: &FileRefOp) -> Result<Arc<TableMetaBlock>> {
        self.table_meta_cache
            .get_or_insert_with::<Error>(key, || Self::get_table_meta_block(file))
    }

    pub fn remove(&self, key: &u32) -> bool {
        self.table_meta_cache.remove(key)
    }

    /// Build the map of meta list, add one block for each default size.
    pub fn build_index(
        block_size: u32,
        block_meta_list: &[BlockMeta],
        meta_list_map: &mut FxHashMap<u32, BlockMeta>,
    ) {
        let mut block_index: u32 = 0;

        for item in block_meta_list.iter() {
            meta_list_map.insert(block_index, item.clone());
            block_index += get_upper_bound(block_size, item.uncompressed_length) / block_size;
        }
    }

    pub fn capacity(&self) -> u64 {
        self.table_meta_cache.capacity()
    }

    // Estimate weight.
    pub fn weighted_size(&self) -> u64 {
        self.table_meta_cache.weight()
    }

    // build and return table meta block.
    pub fn get_table_meta_block(file: &FileRefOp) -> Result<Arc<TableMetaBlock>> {
        let table_meta = Self::get_table_meta(file)?;

        let mut meta_list_map: FxHashMap<u32, BlockMeta> = FxHashMap::default();

        // Build the map of meta list.
        Self::build_index(
            table_meta.data_block_size,
            &table_meta.block_meta,
            &mut meta_list_map,
        );

        Ok(Arc::new(TableMetaBlock {
            table_meta,
            meta_list_map,
        }))
    }

    /// Get the table meta from disk.
    pub fn get_table_meta(file: &FileRefOp) -> Result<TableMeta> {
        let metadata = file.get_ref().metadata()?;
        let file_len = metadata.file_len;

        let mut table_meta_len_bytes = vec![0; BLOCK_RECORD_NUM_OFFSET_USIZE];

        file.get_ref().read(
            &mut table_meta_len_bytes,
            file_len - BLOCK_RECORD_NUM_SIZE_U64,
            BLOCK_RECORD_LENGTH_SIZE_U32,
        )?;

        let table_meta_len: u32 = get_u32_from_bytes!(table_meta_len_bytes, 0);

        let mut table_meta_bytes = vec![0; table_meta_len as usize];

        file.get_ref().read(
            &mut table_meta_bytes,
            file_len - BLOCK_RECORD_NUM_SIZE_U64 - table_meta_len as u64,
            table_meta_len,
        )?;

        let table_meta: TableMeta = TableMeta::decode(table_meta_bytes.as_ref())?;

        Ok(table_meta)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::CStoreConfig;

    #[test]
    fn test() {
        let cstore_config = CStoreConfig::default();

        let cache = TableMetaCache::new(&cstore_config.cache.clone());
        assert_eq!(cache.weighted_size(), 0);

        let table_meta = TableMeta {
            data_block_size: 1,
            compress_type: 1,
            block_meta: vec![],
        };

        let table_meta_block = TableMetaBlock {
            table_meta,
            meta_list_map: FxHashMap::default(),
        };

        let _ = cache
            .table_meta_cache
            .get_or_insert_with::<Error>(&1, || Ok(Arc::new(table_meta_block)));
        let len_1 = cache.weighted_size();
        assert_eq!(len_1, 8);

        let mut meta_list_map_2: FxHashMap<u32, BlockMeta> = FxHashMap::default();
        meta_list_map_2.insert(1, BlockMeta::default());
        let table_meta_block_2 = TableMetaBlock {
            table_meta: TableMeta {
                data_block_size: 1,
                compress_type: 1,
                block_meta: vec![],
            },
            meta_list_map: meta_list_map_2,
        };
        let _ = cache
            .table_meta_cache
            .get_or_insert_with::<Error>(&2, || Ok(Arc::new(table_meta_block_2)));
        assert_eq!(cache.weighted_size(), len_1 * 2 + 20);
    }
}
