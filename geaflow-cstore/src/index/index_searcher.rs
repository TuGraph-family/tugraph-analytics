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

use std::collections::vec_deque::VecDeque;

use dashmap::DashMap;

use crate::{
    api::filter::StoreFilterPushdown, common::GraphDataIndex, config::TableConfig,
    context::EngineContext, get_u32_from_bytes, index::parent_node::ParentNode, log_util::trace,
    lsm::level_controller::LevelController, INDEX_KEY_SIZE, INDEX_VALUE_LEN_SIZE,
};

// TODO: separate fid from SingleIndex.
#[derive(PartialEq, Eq, PartialOrd, Ord)]
pub struct SingleIndex {
    // refer to dict id.
    pub key: u32,

    // refer to struct SecondKey vector.
    pub second_keys: Vec<u8>,

    // refer to is file id.
    pub fid: u32,
}

pub struct IndexSearcher<'a> {
    mem_index: &'a DashMap<u32, ParentNode>,

    table_config: &'a TableConfig,

    engine_context: &'a EngineContext,

    index_granularity: u32,
}

impl<'a> IndexSearcher<'a> {
    pub fn new(
        mem_index: &'a DashMap<u32, ParentNode>,
        table_config: &'a TableConfig,
        engine_context: &'a EngineContext,
        index_granularity: u32,
    ) -> Self {
        IndexSearcher {
            mem_index,
            table_config,
            engine_context,
            index_granularity,
        }
    }

    pub fn get(
        &mut self,
        key: u32,
        level_controller: &LevelController,
        filter_pushdown: &StoreFilterPushdown,
    ) -> Option<Vec<GraphDataIndex>> {
        // get key used in memory index.
        let inner_key = self.get_inner_key(key);

        let mut readable_index_meta_vec = vec![];

        // thread safe, atomic get.
        self.mem_index.entry(inner_key).and_modify(|parent_node| {
            readable_index_meta_vec = parent_node.index_meta_vec.clone();

            // Retain the index files to drop which is readable in level controller.
            readable_index_meta_vec
                .retain(|index_meta| level_controller.is_readable(index_meta.fid));

            // Update the atomic count of the index file in reading.
            level_controller.add_fids_in_read(&readable_index_meta_vec);
        });

        trace!("get parent_node {:?}", readable_index_meta_vec);

        // Fid set
        if !readable_index_meta_vec.is_empty() {
            let graph_data_index_vec = if filter_pushdown.edge_limit.out_edge_limit != u64::MAX
                || filter_pushdown.edge_limit.in_edge_limit != u64::MAX
            {
                self.engine_context
                    .table_searcher
                    .search_index_with_edge_limit(
                        self.table_config,
                        self.engine_context,
                        key,
                        readable_index_meta_vec.as_slice(),
                        filter_pushdown,
                    )
            } else {
                self.engine_context.table_searcher.search_index(
                    self.table_config,
                    self.engine_context,
                    key,
                    readable_index_meta_vec.as_slice(),
                    filter_pushdown,
                )
            };

            level_controller.delete_fids_in_read(&readable_index_meta_vec);

            if graph_data_index_vec.is_empty() {
                None
            } else {
                Some(graph_data_index_vec)
            }
        } else {
            None
        }
    }

    fn get_inner_key(&self, key: u32) -> u32 {
        key / self.index_granularity
    }
}

// get the whole block by fid.
pub fn get_block(fid: u32, bytes: &[u8]) -> VecDeque<SingleIndex> {
    let mut res = VecDeque::new();

    let mut offset_in_block: usize = 0;
    while offset_in_block < bytes.len() {
        let single_index = deserialize_block(fid, bytes, &mut offset_in_block);

        res.push_back(single_index);
    }

    res
}

// deserialize block to SingleIndex one by one.
fn deserialize_block(fid: u32, bytes: &[u8], offset_in_block: &mut usize) -> SingleIndex {
    let key_in_block = get_u32_from_bytes!(bytes, *offset_in_block);

    *offset_in_block += INDEX_KEY_SIZE;

    let value_size = get_u32_from_bytes!(bytes, *offset_in_block) as usize;

    *offset_in_block += INDEX_VALUE_LEN_SIZE;

    let value_in_block = Vec::from(&bytes[*offset_in_block..*offset_in_block + value_size]);
    *offset_in_block += value_size;

    SingleIndex {
        key: key_in_block,

        second_keys: value_in_block,

        fid,
    }
}
