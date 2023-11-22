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
        filter::StoreFilterPushdown,
        graph::{edge::Edge, graph_info_util::is_vertex, graph_serde::GraphSerde},
    },
    engine::dict::id_dict::IdDict,
    iterator::scan_data_iterator_with_edge_limit::ScanDataEdgeLimitedIterator,
    Engine,
};

/// Return EdgeScanIter by calling scan interface.
pub struct EdgeScanIter<'a> {
    graph_serde: &'a GraphSerde,
    engine: &'a Engine,
    scan_data_iterator: Peekable<ScanDataEdgeLimitedIterator<'a>>,
    cur_key: u32,
    cur_origin_key: Vec<u8>,
    vertex_must_contains: bool,
}

impl<'a> EdgeScanIter<'a> {
    pub fn new(
        graph_serde: &'a GraphSerde,
        engine: &'a Engine,
        filter_pushdown: StoreFilterPushdown,
    ) -> Self {
        let vertex_must_contains = filter_pushdown.filter_context.vertex_must_contains;
        Self {
            graph_serde,
            engine,
            scan_data_iterator: engine.scan(Rc::new(filter_pushdown)),
            cur_key: 0,
            cur_origin_key: vec![],
            vertex_must_contains,
        }
    }

    fn get_next_src_id(&mut self) -> Option<(u32, Vec<u8>)> {
        while self.scan_data_iterator.peek().is_some() {
            let first_merged_graph_data = self.scan_data_iterator.next().unwrap();
            let first_key = first_merged_graph_data.key;

            // if vertex exist, the first graph_data should be vertex if, or skip all the
            // edge.
            if !is_vertex(first_merged_graph_data.graph_data.second_key.graph_info) {
                while self.scan_data_iterator.peek().is_some()
                    && self.scan_data_iterator.peek().unwrap().key == first_key
                {
                    // skip all the edge.
                    self.scan_data_iterator.next();
                }
                continue;
            }

            return Some((
                first_key,
                first_merged_graph_data
                    .graph_data
                    .second_key
                    .target_id
                    .clone(),
            ));
        }

        None
    }

    fn get_next_edge_if_must_contains_vertex(&mut self) -> Option<Edge> {
        while self.scan_data_iterator.peek().is_some() {
            if self.cur_key != self.scan_data_iterator.peek().unwrap().key {
                let key_and_next_vertex_opt = self.get_next_src_id();
                key_and_next_vertex_opt.as_ref()?;

                let key_and_next_vertex = key_and_next_vertex_opt.unwrap();
                self.cur_key = key_and_next_vertex.0;
                self.cur_origin_key = key_and_next_vertex.1;
            } else {
                let next_merged_graph_data = self.scan_data_iterator.next().unwrap();
                if is_vertex(next_merged_graph_data.graph_data.second_key.graph_info) {
                    continue;
                }

                return Some(self.graph_serde.deserialize_edge(
                    self.cur_origin_key.as_slice(),
                    &next_merged_graph_data.graph_data,
                    &self.engine.label_dict,
                ));
            }
        }

        None
    }

    fn get_next_edge(&mut self) -> Option<Edge> {
        while self.scan_data_iterator.peek().is_some() {
            if self.cur_origin_key.is_empty()
                || self.cur_key != self.scan_data_iterator.peek().unwrap().key
            {
                let next_merged_graph_data = self.scan_data_iterator.next().unwrap();
                self.cur_origin_key = self
                    .engine
                    .id_dict
                    .get_origin_key(next_merged_graph_data.key)
                    .unwrap();
                self.cur_key = next_merged_graph_data.key;

                if is_vertex(next_merged_graph_data.graph_data.second_key.graph_info) {
                    continue;
                } else {
                    return Some(self.graph_serde.deserialize_edge(
                        self.cur_origin_key.as_slice(),
                        &next_merged_graph_data.graph_data,
                        &self.engine.label_dict,
                    ));
                }
            } else {
                let next_merged_graph_data = self.scan_data_iterator.next().unwrap();
                if is_vertex(next_merged_graph_data.graph_data.second_key.graph_info) {
                    continue;
                }

                return Some(self.graph_serde.deserialize_edge(
                    self.cur_origin_key.as_slice(),
                    &next_merged_graph_data.graph_data,
                    &self.engine.label_dict,
                ));
            }
        }

        None
    }
}

impl<'a> Iterator for EdgeScanIter<'a> {
    type Item = Edge;

    fn next(&mut self) -> Option<Self::Item> {
        if self.scan_data_iterator.peek().is_some() {
            return if self.vertex_must_contains {
                self.get_next_edge_if_must_contains_vertex()
            } else {
                self.get_next_edge()
            };
        }

        None
    }
}
