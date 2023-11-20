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

use bytes::BufMut;
use dashmap::DashMap;

use crate::{
    common::GraphDataIndex,
    config::TableConfig,
    index::parent_node::{IndexMeta, ParentNode},
    lsm::{level_controller::LevelController, level_handler::TableInfo, lsm_freezer::LsmFreezer},
    serialize_util::{serialize_index, SerializeType},
    Result, TableType, WritableTable, INDEX_BLOCK_BINARY_HEADER,
};

pub struct IndexBuilder<'a> {
    // TODO: need optimize memory index by CSR.
    mem_index: &'a DashMap<u32, ParentNode>,

    index_block_builder: IndexBlockBuilder,

    index_table_builder: IndexTableBuilder<'a>,

    index_granularity: u32,
}

impl<'a> IndexBuilder<'a> {
    pub fn new(
        mem_index: &'a DashMap<u32, ParentNode>,
        index_granularity: u32,
        level_controller: &'a LevelController,
        table_config: &'a TableConfig,
        level: usize,
    ) -> Result<Self> {
        Ok(IndexBuilder {
            mem_index,
            index_block_builder: IndexBlockBuilder::new(index_granularity),
            index_table_builder: IndexTableBuilder::new(level_controller, table_config, level)?,
            index_granularity,
        })
    }

    pub fn put(&mut self, key: u32, value: GraphDataIndex, is_hot_spot: bool) -> Result<()> {
        let inner_value = serialize_index(SerializeType::ISerde, &value);

        if self.index_block_builder.is_empty() {
            self.index_block_builder.with_bound(key);
        } else {
            // freeze previous index, to separate hot spot index block.
            if is_hot_spot {
                self.freeze()?;
            }
        }

        if is_hot_spot {
            self.index_block_builder.put(key, inner_value);
            self.freeze()?;

            return Ok(());
        }

        if key >= self.index_block_builder.lower_bound && key < self.index_block_builder.upper_bound
        {
            self.index_block_builder.put(key, inner_value);
        } else {
            self.freeze()?;
            self.index_block_builder.with_bound(key);

            self.index_block_builder.put(key, inner_value);
        }

        Ok(())
    }

    pub fn flush(&mut self, data_table_fid: u32, data_table_size: u64) -> Result<TableInfo> {
        self.freeze()?;

        self.index_table_builder
            .flush(data_table_fid, data_table_size)
    }

    pub fn reach_capacity_threshold(&self) -> bool {
        self.index_table_builder.reach_capacity_threshold()
    }

    fn freeze(&mut self) -> Result<()> {
        let start_key = self.index_block_builder.start_end_key.0;
        let index_meta = self.index_table_builder.freeze(
            self.index_block_builder.serialize_block(),
            self.index_block_builder.start_end_key,
        )?;
        self.index_block_builder.reset();
        // append delta data.
        self.append_mem_index(self.get_inner_key(start_key), index_meta);

        Ok(())
    }

    // thread safe, atomic append.
    fn append_mem_index(&self, inner_key: u32, index_meta: IndexMeta) {
        self.mem_index
            .entry(inner_key)
            .or_default()
            .append(index_meta);

        // TODO, Temporarily unused, switch CsrMemIndex after optimization.
        // self.mem_index.apply(inner_key, |parent_node| {
        //     parent_node.append(index_meta);
        // });
    }

    fn get_inner_key(&self, key: u32) -> u32 {
        key / self.index_granularity
    }
}

pub struct IndexBlockBuilder {
    // key to serialized index.
    block: Vec<(u32, Vec<u8>)>,

    // lower bound in block.
    pub lower_bound: u32,

    // upper bound in block.
    pub upper_bound: u32,

    // start and end key in block.
    pub start_end_key: (u32, u32),

    index_granularity: u32,

    serialized_size: usize,
}

impl IndexBlockBuilder {
    pub fn new(index_granularity: u32) -> Self {
        IndexBlockBuilder {
            block: vec![],
            lower_bound: 0,
            upper_bound: 0,
            start_end_key: (u32::MAX, u32::MIN),
            index_granularity,
            serialized_size: 0,
        }
    }

    pub fn put(&mut self, key: u32, value: Vec<u8>) {
        self.serialized_size += INDEX_BLOCK_BINARY_HEADER + value.len();

        self.block.push((key, value));

        // update start end key.
        if self.start_end_key.0 == u32::MAX {
            self.start_end_key.0 = key;
        }
        self.start_end_key.1 = key;
    }

    pub fn with_bound(&mut self, key: u32) {
        self.lower_bound = self.lower_bound(key);
        self.upper_bound = self.lower_bound + self.index_granularity;
    }

    pub fn is_empty(&self) -> bool {
        self.block.is_empty()
    }

    pub fn serialize_block(&mut self) -> Vec<u8> {
        let mut res: Vec<u8> = Vec::with_capacity(self.serialized_size);
        for (key, value) in &self.block {
            res.put_u32(*key);

            res.put_u32(value.len() as u32);
            res.put_slice(value.as_slice());
        }

        res
    }

    pub fn reset(&mut self) {
        self.block.clear();
        self.lower_bound = 0;
        self.upper_bound = 0;
        self.start_end_key = (u32::MAX, u32::MIN);
    }

    fn lower_bound(&self, key: u32) -> u32 {
        key / self.index_granularity * self.index_granularity
    }
}

pub struct IndexTableBuilder<'a> {
    // Info of IndexTable start
    // Fid of the table flushing index.
    index_table_fid: u32,

    // Record length of index flushed.
    index_table_uncompressed_size: u64,

    // WritableTable of Index, which is used to freeze index into disk.
    index_table: WritableTable<'a>,

    // Start and end key in table.
    start_end_key: (u32, u32),
    // Info of IndexTable end

    // capacity threshold.
    capacity: u64,
}

impl<'a> IndexTableBuilder<'a> {
    pub fn new(
        level_controller: &'a LevelController,
        table_config: &'a TableConfig,
        level: usize,
    ) -> Result<Self> {
        let table: WritableTable =
            level_controller.create_new_table(table_config, TableType::Is)?;

        Ok(IndexTableBuilder {
            index_table_fid: table.get_fid(),
            index_table: table,
            index_table_uncompressed_size: 0,
            start_end_key: (u32::MAX, u32::MIN),
            capacity: level_controller.get_level_sizes()[level],
        })
    }

    pub fn freeze(
        &mut self,
        index_block_bytes: Vec<u8>,
        start_end_key: (u32, u32),
    ) -> Result<IndexMeta> {
        let (table_offset, index_block_bytes_len) =
            LsmFreezer::freeze_index(&mut self.index_table, index_block_bytes)?;

        // table size can't be accurately calculated before flush,
        // just plus all index_block_len as table_size.
        self.index_table_uncompressed_size =
            self.index_table.get_uncompressed_table_offset() + index_block_bytes_len as u64;

        self.start_end_key.0 = u32::min(self.start_end_key.0, start_end_key.0);
        self.start_end_key.1 = u32::max(self.start_end_key.1, start_end_key.1);

        Ok(IndexMeta {
            fid: self.index_table_fid,
            offset: table_offset,
        })
    }

    pub fn flush(&mut self, data_table_fid: u32, data_table_size: u64) -> Result<TableInfo> {
        let table_info = LsmFreezer::flush_index(
            self.index_table_fid,
            &mut self.index_table,
            self.start_end_key,
            data_table_fid,
            data_table_size,
        )?;

        // close table after use.
        self.index_table.close();

        Ok(table_info)
    }

    pub fn reach_capacity_threshold(&self) -> bool {
        self.index_table_uncompressed_size >= self.capacity
    }
}
