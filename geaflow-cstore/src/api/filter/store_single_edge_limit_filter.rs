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

use std::sync::Mutex;

use crate::api::{
    filter::{
        filter_graph_info, filter_graph_meta, store_or_filter::StoreOrFilter, EdgeLimit, IEdgeLimit,
    },
    graph::graph_info_util::{is_in_edge, is_in_edge_graph_meta, is_vertex, is_vertex_graph_meta},
};

pub struct StoreSingleEdgeLimitFilter {
    filter: StoreOrFilter,
    edge_limit: EdgeLimit,
    filter_num: usize,
    // TODO: need optimize mutex.
    out_edge_counts: Mutex<Vec<u64>>,
    in_edge_counts: Mutex<Vec<u64>>,
}

impl StoreSingleEdgeLimitFilter {
    pub fn new(filter: StoreOrFilter, edge_limit: EdgeLimit) -> Self {
        let mut out_edge_counts = vec![];
        let mut in_edge_counts = vec![];

        for _i in 0..filter.get_or_filter().len() {
            out_edge_counts.push(0);
            in_edge_counts.push(0);
        }
        Self {
            filter,
            edge_limit,
            filter_num: out_edge_counts.len(),
            out_edge_counts: Mutex::new(out_edge_counts),
            in_edge_counts: Mutex::new(in_edge_counts),
        }
    }

    pub fn keep_edge(&self, index: usize) -> bool {
        self.keep_out_edge(index) && self.keep_in_edge(index)
    }

    pub fn keep_out_edge(&self, index: usize) -> bool {
        *self.out_edge_counts.lock().unwrap().get(index).unwrap() <= self.edge_limit.out_edge_limit
    }

    pub fn keep_in_edge(&self, index: usize) -> bool {
        *self.in_edge_counts.lock().unwrap().get(index).unwrap() <= self.edge_limit.in_edge_limit
    }
}

impl IEdgeLimit for StoreSingleEdgeLimitFilter {
    fn select_graph_info(&self, graph_info: u64) -> bool {
        for pair in self.filter.get_or_filter().iter().enumerate() {
            if filter_graph_info(pair.1, graph_info) {
                if is_vertex(graph_info) {
                    return true;
                }

                return if is_in_edge(graph_info) {
                    let count = *self.in_edge_counts.lock().unwrap().get(pair.0).unwrap();
                    self.in_edge_counts
                        .lock()
                        .unwrap()
                        .insert(pair.0, count + 1);

                    self.keep_in_edge(pair.0)
                } else {
                    let count = *self.out_edge_counts.lock().unwrap().get(pair.0).unwrap();
                    self.out_edge_counts
                        .lock()
                        .unwrap()
                        .insert(pair.0, count + 1);

                    self.keep_out_edge(pair.0)
                };
            }
        }

        false
    }

    fn select_graph_meta(&self, graph_meta: &[u8]) -> bool {
        for pair in self.filter.get_or_filter().iter().enumerate() {
            if filter_graph_meta(pair.1, graph_meta) {
                if is_vertex_graph_meta(graph_meta) {
                    return true;
                }

                return if is_in_edge_graph_meta(graph_meta) {
                    let count = *self.in_edge_counts.lock().unwrap().get(pair.0).unwrap();
                    self.in_edge_counts
                        .lock()
                        .unwrap()
                        .insert(pair.0, count + 1);

                    self.keep_in_edge(pair.0)
                } else {
                    let count = *self.out_edge_counts.lock().unwrap().get(pair.0).unwrap();
                    self.out_edge_counts
                        .lock()
                        .unwrap()
                        .insert(pair.0, count + 1);

                    self.keep_out_edge(pair.0)
                };
            }
        }

        false
    }

    fn drop_all_remaining(&self) -> bool {
        for index in 0..self.filter_num {
            if self.keep_edge(index) {
                return false;
            }
        }

        true
    }

    fn reset(&self) {
        for i in 0..self.filter_num {
            self.out_edge_counts.lock().unwrap().insert(i, 0);
            self.in_edge_counts.lock().unwrap().insert(i, 0);
        }
    }
}
