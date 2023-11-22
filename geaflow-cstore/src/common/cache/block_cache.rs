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

use std::{num::NonZeroU32, sync::Arc, time::Instant};

use quick_cache::{sync::Cache, Weighter};

use super::file_cache::FileRefOp;
use crate::{
    config::CacheConfig,
    error::Error,
    log_util::trace,
    table::Block,
    util::compress_util::{decompress, DecompressHandler},
    Result,
};

pub struct BlockInitParameter<'a> {
    pub table_physical_offset: u64,
    pub compressed_length: u32,
    pub uncompressed_length: u32,
    pub file: &'a FileRefOp,
    pub decompress_handler: &'a mut DecompressHandler,
}

pub struct BlockCache {
    block_cache: Cache<u64, Arc<Block>, BlockWeigher>,
}

#[derive(Clone)]
pub struct BlockWeigher;

// Define the weigher of block cache, which is used to calculate the weight of
// each block put into cache.
impl Weighter<u64, (), Arc<Block>> for BlockWeigher {
    fn weight(&self, _key: &u64, _qey: &(), val: &Arc<Block>) -> NonZeroU32 {
        NonZeroU32::new(val.get_read_buffer().len().clamp(1, u32::MAX as usize) as u32).unwrap()
    }
}

impl BlockCache {
    pub fn new(cache_config: &CacheConfig) -> Self {
        let cache: Cache<u64, Arc<Block>, BlockWeigher> = Cache::with_weighter(
            cache_config.block_cache_item_number,
            cache_config.block_cache_capacity,
            BlockWeigher,
        );

        BlockCache { block_cache: cache }
    }

    // TODO compare the performance: 1. get_with(|init|) 2. init + get_with
    pub fn get_with(&self, key: &u64, parameter: &mut BlockInitParameter) -> Result<Arc<Block>> {
        self.block_cache
            .get_or_insert_with::<Error>(key, || Self::get_block_buffer(parameter))
    }

    pub fn capacity(&self) -> u64 {
        self.block_cache.capacity()
    }

    // Estimate weight.
    pub fn weighted_size(&self) -> u64 {
        self.block_cache.weight()
    }

    pub fn get_block_buffer(parameter: &mut BlockInitParameter) -> Result<Arc<Block>> {
        // A block with compressed length buffer filled default data, which can be used
        // to read data of exact length.
        let mut compressed_buffer = vec![0u8; parameter.compressed_length as usize];

        // A block with uncompressed length buffer, which can engine the data
        // decompressed by the compressed block.
        let mut raw_buffer = vec![0u8; parameter.uncompressed_length as usize];

        // Read all the data to the compressed block.
        let start_disk_io = Instant::now();
        parameter.file.get_ref().read(
            &mut compressed_buffer,
            parameter.table_physical_offset,
            parameter.compressed_length,
        )?;
        let disk_io_cost = (Instant::now() - start_disk_io).as_micros();

        // Decompress the data.
        let start_decompress_data = Instant::now();
        decompress(
            parameter.decompress_handler,
            &mut raw_buffer,
            &compressed_buffer,
            parameter.compressed_length,
        );
        trace!(
            "get block buffer, read {}bytes, disk IO cost {}us, decompress data cost {}us",
            parameter.compressed_length,
            disk_io_cost,
            (Instant::now() - start_decompress_data).as_micros()
        );

        Ok(Arc::new(Block::new(raw_buffer)))
    }
}
