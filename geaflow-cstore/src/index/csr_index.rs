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

use std::{
    mem,
    sync::atomic::{AtomicU64, Ordering},
};

use dashmap::DashMap;

use super::index_modifier::IndexModifier;
use crate::{
    config::{IndexConfig, TableConfig},
    context::EngineContext,
    index::{
        index_builder::IndexBuilder,
        index_searcher::IndexSearcher,
        parent_node::{IndexMeta, ParentNode},
    },
    lsm::level_controller::LevelController,
    metric,
    metric::MetricUpdater,
    Result,
};

const INDEX_META_SIZE: usize = mem::size_of::<IndexMeta>();

pub struct CsrIndex {
    // TODO: need handle concurrent conflict.
    mem_index: DashMap<u32, ParentNode>,

    pub index_granularity: u32,
    pub last_cache_weight: AtomicU64,
}

impl CsrIndex {
    pub fn new(index: &IndexConfig) -> Self {
        CsrIndex {
            mem_index: DashMap::new(),
            index_granularity: index.index_granularity,
            last_cache_weight: AtomicU64::new(0),
        }
    }

    // 1. index builder is thread safe, just need handle concurrent
    // conflict for mem_index.
    // 2. one index builder refer to one table.
    pub fn create_index_builder<'a>(
        &'a self,
        level_controller: &'a LevelController,
        table_config: &'a TableConfig,
        level: usize,
    ) -> Result<IndexBuilder> {
        IndexBuilder::new(
            &self.mem_index,
            self.index_granularity,
            level_controller,
            table_config,
            level,
        )
    }

    // index searcher is thread safe.
    pub fn create_index_searcher<'a>(
        &'a self,
        table_config: &'a TableConfig,
        engine_context: &'a EngineContext,
    ) -> IndexSearcher {
        IndexSearcher::new(
            &self.mem_index,
            table_config,
            engine_context,
            self.index_granularity,
        )
    }

    // Create the modifier of csr index without creating new table.
    pub fn create_index_modifier(&self) -> IndexModifier {
        IndexModifier::new(&self.mem_index)
    }

    pub fn weight_size(&self) -> u64 {
        let mut sum: usize = 0;
        self.mem_index
            .iter()
            .for_each(|entry| sum += entry.index_meta_vec.len());

        (sum * INDEX_META_SIZE) as u64
    }

    pub fn close(&self) {
        self.mem_index.clear();
    }
}

impl MetricUpdater for CsrIndex {
    fn update(&self) {
        let current_weight_size = self.weight_size();
        let delta = current_weight_size - self.last_cache_weight.load(Ordering::SeqCst);
        self.last_cache_weight
            .store(current_weight_size, Ordering::SeqCst);
        metric::CSR_INDEX_MAP_MEMORY.increment(delta as f64);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::{
        api::filter::create_empty_filter_pushdown,
        common::GraphDataIndex,
        config::CStoreConfigBuilder,
        engine::dict::label_dict::LabelDict,
        lsm::level_controller::LevelController,
        test_util::{bind_job_name_with_ts, delete_test_dir},
        ConfigMap,
    };

    const LOCAL_ITERATIONS: u32 = 10000;

    fn test_csr(config_map: &ConfigMap, iterations: u32) {
        let cstore = CStoreConfigBuilder::default()
            .set_with_map(config_map)
            .build();

        let label_dict = Arc::new(LabelDict::default());
        let engine_context = Arc::new(EngineContext::new(&label_dict, &cstore));
        let csr_index: CsrIndex = CsrIndex::new(&cstore.index);

        let level_controller = Arc::new(LevelController::new(
            &cstore.store,
            Arc::clone(&engine_context.file_operator),
        ));
        let mut index_builder = csr_index
            .create_index_builder(&level_controller, &cstore.table, 0)
            .unwrap();

        for i in 1..iterations {
            let graph_data_index: GraphDataIndex = GraphDataIndex {
                graph_data_fid: i,
                graph_data_offset: 1,
                all_filtered: false,
                second_keys: Vec::new(),
            };
            index_builder.put(i, graph_data_index, false).unwrap();
        }

        let table_info = index_builder.flush(0, 0).unwrap();
        level_controller.register_to_lsm(&vec![table_info], 0);

        for i in 1..iterations {
            let res = csr_index
                .create_index_searcher(&cstore.table, &engine_context)
                .get(i, &level_controller, &create_empty_filter_pushdown());
            assert!(res.is_some());
        }

        delete_test_dir(&cstore.table, &cstore.persistent);
    }

    #[test]
    fn test_csr_local() {
        let mut config_map = ConfigMap::default();
        let job_name = bind_job_name_with_ts("test_csr_local");
        config_map.insert("store.job_name", job_name.as_str());

        test_csr(&config_map, LOCAL_ITERATIONS);
    }
}
