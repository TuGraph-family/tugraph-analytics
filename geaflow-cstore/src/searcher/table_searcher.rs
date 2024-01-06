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

use std::{collections::VecDeque, sync::Arc, time::Instant};

use dashmap::DashMap;
use itertools::Itertools;
use rustc_hash::FxHashMap;

use crate::{
    api::{
        filter::{create_empty_filter_pushdown, FilterContext, StoreFilterPushdown},
        graph::graph_info_util::is_vertex,
    },
    config::TableConfig,
    error_and_panic, get_u32_from_bytes,
    index::parent_node::IndexMeta,
    log_util::trace,
    searcher::graph_data_index_selector::GraphDataIndexSelector,
    serialize_util::{deserialize_index_with_filter_pushdown, SerializeType},
    util::thread_util::RayonThreadPool,
    CStoreConfig, EngineContext, GraphData, GraphDataIndex, ReadableTable, Result, TableContext,
    TableType, INDEX_KEY_SIZE, INDEX_VALUE_LEN_SIZE,
};

const TABLE_PARALLEL_SEARCH_THRESHOLD: usize = 4;

pub struct TableSearcher {
    searcher_pool: RayonThreadPool,
}

impl TableSearcher {
    pub fn new(cstore_config: &CStoreConfig) -> Self {
        Self {
            searcher_pool: RayonThreadPool::new(cstore_config.store.io_thread_num),
        }
    }
}

impl TableSearcher {
    pub fn search_index(
        &self,
        table_config: &TableConfig,
        engine_context: &EngineContext,
        key: u32,
        to_search: &[IndexMeta],
        filter_pushdown: &StoreFilterPushdown,
    ) -> Vec<GraphDataIndex> {
        let mut graph_data_index_vec = vec![];

        let start = Instant::now();
        for index_meta in to_search {
            if let Some(graph_data_index) = self.get_graph_data_index_with_filter(
                index_meta,
                table_config,
                engine_context,
                key,
                filter_pushdown,
            ) {
                graph_data_index_vec.push(graph_data_index);
            }
        }

        trace!(
            "search index cost {}us",
            (Instant::now() - start).as_micros()
        );
        graph_data_index_vec
    }

    pub fn search_index_with_edge_limit(
        &self,
        table_config: &TableConfig,
        engine_context: &EngineContext,
        key: u32,
        to_search: &[IndexMeta],
        filter_pushdown: &StoreFilterPushdown,
    ) -> Vec<GraphDataIndex> {
        // TODO:
        // 1. support multi thread search.
        // 2. don't need read all file.
        // 2.1 graph data in single file is sorted.
        // 2.2 graph data in files of level1-7 is sorted.
        let mut graph_data_index_vec = vec![];

        // deserialize and filter.
        for index_meta in to_search {
            if let Some(graph_data_index) = self.get_graph_data_index_with_filter(
                index_meta,
                table_config,
                engine_context,
                key,
                &create_empty_filter_pushdown(),
            ) {
                graph_data_index_vec.push(graph_data_index)
            }
        }

        // select edge.
        let mut graph_data_index_selector = GraphDataIndexSelector::new(
            graph_data_index_vec.as_mut(),
            engine_context.sort_field.as_ref(),
        );

        graph_data_index_selector.select(filter_pushdown);

        // filter graph data index that is all filtered.
        graph_data_index_vec
            .into_iter()
            .filter(|graph_data_index| !graph_data_index.all_filtered)
            .collect_vec()
    }

    fn get_graph_data_index_with_filter(
        &self,
        index_meta: &IndexMeta,
        table_config: &TableConfig,
        engine_context: &EngineContext,
        key: u32,
        filter_pushdown: &StoreFilterPushdown,
    ) -> Option<GraphDataIndex> {
        let index_bytes = TableSearcher::search_from_table(
            table_config,
            engine_context,
            TableType::Is,
            index_meta.fid,
            index_meta.offset,
            true,
        )
        .unwrap_or_else(|e| {
            error_and_panic!("error occurred in searching index: {}", e);
        });

        if let Some(serialized_graph_data_index) =
            TableSearcher::search_in_block(key, index_bytes.first().as_ref().unwrap())
        {
            let graph_data_index = deserialize_index_with_filter_pushdown(
                SerializeType::ISerde,
                serialized_graph_data_index.as_slice(),
                &filter_pushdown.filter_handler,
                &engine_context.ttl_dict,
            );
            return Some(graph_data_index);
        }

        None
    }

    pub fn search_and_attach_property(
        &self,
        table_config: &TableConfig,
        engine_context: &EngineContext,
        graph_data_index: GraphDataIndex,
        enable_cache: bool,
        filter_context: &FilterContext,
    ) -> Vec<GraphData> {
        let properties = TableSearcher::search_from_table(
            table_config,
            engine_context,
            TableType::Vs,
            graph_data_index.graph_data_fid,
            graph_data_index.graph_data_offset,
            enable_cache,
        )
        .unwrap_or_else(|e| {
            error_and_panic!("error occurred in searching property: {}", e);
        });

        let mut graph_data_vec = vec![];
        let mut second_key_iter = graph_data_index.second_keys.into_iter();
        for property in properties.into_iter() {
            let second_key = second_key_iter.next().unwrap();

            let is_vertex = is_vertex(second_key.graph_info);
            let graph_data = if (is_vertex && filter_context.drop_vertex_value_property)
                || (!is_vertex && filter_context.drop_edge_value_property)
            {
                GraphData {
                    property: vec![],
                    second_key,
                }
            } else {
                GraphData {
                    property,
                    second_key,
                }
            };
            graph_data_vec.push(graph_data);
        }
        assert!(second_key_iter.next().is_none());

        graph_data_vec
    }

    // Use parallel search if fid num is greater than
    // TABLE_PARALLEL_SEARCH_THRESHOLD.
    pub fn try_search_and_attach_property_in_parallel(
        &self,
        table_config: &TableConfig,
        engine_context: &EngineContext,
        graph_data_index_vec: Vec<GraphDataIndex>,
        filter_pushdown: &StoreFilterPushdown,
    ) -> Arc<DashMap<u32, VecDeque<GraphData>>> {
        let mut fid_to_graph_data: Arc<DashMap<u32, VecDeque<GraphData>>> =
            Arc::new(DashMap::new());

        if graph_data_index_vec.len() > TABLE_PARALLEL_SEARCH_THRESHOLD {
            fid_to_graph_data = self.search_and_attach_property_in_parallel(
                table_config,
                engine_context,
                graph_data_index_vec,
                &filter_pushdown.filter_context,
            );
        } else {
            for graph_data_index in graph_data_index_vec {
                fid_to_graph_data
                    .entry(graph_data_index.graph_data_fid)
                    .or_default()
                    .extend(
                        self.search_and_attach_property(
                            table_config,
                            engine_context,
                            graph_data_index,
                            true,
                            &filter_pushdown.filter_context,
                        )
                        .into_iter()
                        .filter(|graph_data| !graph_data.second_key.is_empty()),
                    );
            }
        }

        fid_to_graph_data
    }

    fn search_and_attach_property_in_parallel(
        &self,
        table_config: &TableConfig,
        engine_context: &EngineContext,
        graph_data_index_vec: Vec<GraphDataIndex>,
        filter_context: &FilterContext,
    ) -> Arc<DashMap<u32, VecDeque<GraphData>>> {
        let start_parallel_search = Instant::now();
        let fid_to_graph_data: Arc<DashMap<u32, VecDeque<GraphData>>> = Arc::new(DashMap::new());

        let mut fid_to_graph_data_index_vec: FxHashMap<u32, Vec<GraphDataIndex>> =
            FxHashMap::default();
        for graph_data_index in graph_data_index_vec {
            fid_to_graph_data_index_vec
                .entry(graph_data_index.graph_data_fid)
                .or_default()
                .push(graph_data_index);
        }

        self.searcher_pool.scope(|s| {
            for new_graph_data_index_vec in fid_to_graph_data_index_vec {
                let fid_to_graph_data_cloned = Arc::clone(&fid_to_graph_data);
                let fid_cloned = new_graph_data_index_vec.0;
                s.spawn(move |_| {
                    for graph_data_index in new_graph_data_index_vec.1 {
                        let graph_data_vec = self.search_and_attach_property(
                            table_config,
                            engine_context,
                            graph_data_index,
                            true,
                            filter_context,
                        );

                        fid_to_graph_data_cloned
                            .entry(fid_cloned)
                            .or_default()
                            .extend(
                                graph_data_vec
                                    .into_iter()
                                    .filter(|graph_data| !graph_data.second_key.is_empty()),
                            );
                    }
                });
            }
        });
        trace!(
            "parallel search cost {}us",
            (Instant::now() - start_parallel_search).as_micros()
        );

        fid_to_graph_data
    }

    fn search_from_table(
        table_config: &TableConfig,
        engine_context: &EngineContext,
        table_type: TableType,
        fid: u32,
        offset: u64,
        enable_cache: bool,
    ) -> Result<Vec<Vec<u8>>> {
        let mut table = ReadableTable::new(
            table_config,
            TableContext::new(table_config, fid, &engine_context.file_operator)
                .with_table_type(table_type)
                .with_enable_cache(enable_cache),
            &engine_context.cache_handler,
        )?;

        let start_get_table = Instant::now();
        let values = table.get(offset)?;
        trace!(
            "get table cost {}us",
            (Instant::now() - start_get_table).as_micros()
        );

        // close table when finish searching.
        table.close();

        Ok(values)
    }

    pub fn search_in_block(key: u32, bytes: &[u8]) -> Option<Vec<u8>> {
        let mut offset_in_block: usize = 0;
        while offset_in_block < bytes.len() {
            let value = TableSearcher::deserialize_block_by_key(key, bytes, &mut offset_in_block);
            if value.is_some() {
                return value;
            }
        }

        None
    }

    // deserialize block to SingleIndex one by one by key.
    fn deserialize_block_by_key(
        key: u32,
        bytes: &[u8],
        offset_in_block: &mut usize,
    ) -> Option<Vec<u8>> {
        let key_in_block = get_u32_from_bytes!(bytes, *offset_in_block);
        *offset_in_block += INDEX_KEY_SIZE;

        let value_size = get_u32_from_bytes!(bytes, *offset_in_block) as usize;
        *offset_in_block += INDEX_VALUE_LEN_SIZE;

        if key == key_in_block {
            let value_in_block = Vec::from(&bytes[*offset_in_block..*offset_in_block + value_size]);

            Some(value_in_block)
        } else {
            *offset_in_block += value_size;

            None
        }
    }
}
