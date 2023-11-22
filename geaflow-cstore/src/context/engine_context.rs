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

use std::sync::Arc;

use crate::{
    api::graph::graph_comparator::{parse_graph_sort_field, GraphSortField},
    cache::CStoreCacheHandler,
    config::CStoreConfig,
    engine::dict::{label_dict::LabelDict, ttl_dict::TtlDict},
    error_and_panic,
    file::file_operator::CstoreFileOperator,
    metric::{MetricCollector, MetricUpdater},
    searcher::table_searcher::TableSearcher,
};

/// Context of engine engine, which mainly stores some handles or data used
/// globally.
pub struct EngineContext {
    pub ttl_dict: Arc<TtlDict>,
    pub cache_handler: Arc<CStoreCacheHandler>,
    pub table_searcher: TableSearcher,
    pub sort_field: Vec<GraphSortField>,
    pub file_operator: Arc<CstoreFileOperator>,
    pub metric_collector: MetricCollector,
}

impl EngineContext {
    pub fn new(label_dict: &LabelDict, cstore_config: &CStoreConfig) -> Self {
        let ttl_dict = Arc::new(TtlDict::new(label_dict, &cstore_config.store));
        let file_operator =
            CstoreFileOperator::new(&cstore_config.persistent).unwrap_or_else(|e| {
                error_and_panic!("{}", e);
            });

        let cache_handler = Arc::new(CStoreCacheHandler::new(&cstore_config.cache));
        let metric_collector = MetricCollector::new(&cstore_config.metric);
        let updater: Arc<dyn MetricUpdater> = Arc::clone(&cache_handler) as _;
        metric_collector.register(updater);

        Self {
            ttl_dict,
            cache_handler,
            table_searcher: TableSearcher::new(cstore_config),
            sort_field: parse_graph_sort_field(cstore_config.store.sort_field.as_str()),
            file_operator: Arc::new(file_operator),
            metric_collector,
        }
    }
}
