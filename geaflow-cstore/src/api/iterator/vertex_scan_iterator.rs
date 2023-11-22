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

use std::{iter::Peekable, rc::Rc};

use crate::{
    api::{
        filter::{
            store_and_filter::StoreAndFilter, store_only_vertex_filter::StoreOnlyVertexFilter,
            EdgeLimit, FilterHandler, StoreFilterPushdown,
        },
        graph::{graph_serde::GraphSerde, vertex::Vertex},
    },
    iterator::scan_data_iterator_with_edge_limit::ScanDataEdgeLimitedIterator,
    Engine,
};

/// Return VertexScanIter by calling scan interface.
pub struct VertexScanIter<'a> {
    graph_serde: &'a GraphSerde,
    engine: &'a Engine,
    scan_data_merge_iterator: Peekable<ScanDataEdgeLimitedIterator<'a>>,
}

impl<'a> VertexScanIter<'a> {
    pub fn new(
        graph_serde: &'a GraphSerde,
        engine: &'a Engine,
        filter_pushdown: StoreFilterPushdown,
    ) -> Self {
        Self {
            graph_serde,
            engine,
            scan_data_merge_iterator: engine.scan(Rc::new(VertexScanIter::generate_new_pushdown(
                filter_pushdown,
            ))),
        }
    }

    fn generate_new_pushdown(filter_pushdown: StoreFilterPushdown) -> StoreFilterPushdown {
        let only_vertex_filter = FilterHandler::OnlyVertex(StoreOnlyVertexFilter::default());

        let mut filter_vec = vec![filter_pushdown.filter_handler];
        filter_vec.push(only_vertex_filter);

        let new_filter_handler = FilterHandler::And(StoreAndFilter::new(filter_vec));
        StoreFilterPushdown::new(
            new_filter_handler,
            filter_pushdown.filter_context,
            EdgeLimit::default(),
        )
    }
}

impl<'a> Iterator for VertexScanIter<'a> {
    type Item = Vertex;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(merged_graph_data) = self.scan_data_merge_iterator.next() {
            return Some(self.graph_serde.deserialize_vertex_in_scan(
                &merged_graph_data.graph_data,
                &self.engine.label_dict,
            ));
        }

        None
    }
}
