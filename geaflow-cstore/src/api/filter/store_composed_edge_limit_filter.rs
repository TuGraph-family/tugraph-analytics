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

use std::cell::Cell;

use crate::api::{
    filter::{filter_graph_info, filter_graph_meta, EdgeLimit, FilterHandler, IEdgeLimit},
    graph::graph_info_util::{is_in_edge, is_in_edge_graph_meta, is_vertex, is_vertex_graph_meta},
};

pub struct StoreComposedEdgeLimitFilter {
    filter: Box<FilterHandler>,
    edge_limit: EdgeLimit,
    out_edge_count: Cell<u64>,
    in_edge_count: Cell<u64>,
}

impl StoreComposedEdgeLimitFilter {
    pub fn new(filter: Box<FilterHandler>, edge_limit: EdgeLimit) -> Self {
        Self {
            filter,
            edge_limit,
            out_edge_count: Cell::new(0),
            in_edge_count: Cell::new(0),
        }
    }

    pub fn keep_edge(&self) -> bool {
        self.out_edge_count.get() <= self.edge_limit.out_edge_limit
            || self.in_edge_count.get() <= self.edge_limit.in_edge_limit
    }

    pub fn keep_in_edge(&self) -> bool {
        self.in_edge_count.get() <= self.edge_limit.in_edge_limit
    }

    pub fn keep_out_edge(&self) -> bool {
        self.out_edge_count.get() <= self.edge_limit.out_edge_limit
    }
}

impl IEdgeLimit for StoreComposedEdgeLimitFilter {
    fn select_graph_info(&self, graph_info: u64) -> bool {
        if filter_graph_info(&self.filter, graph_info) {
            if is_vertex(graph_info) {
                return true;
            }

            if is_in_edge(graph_info) {
                self.in_edge_count.set(self.in_edge_count.get() + 1);
                self.keep_in_edge()
            } else {
                self.out_edge_count.set(self.out_edge_count.get() + 1);
                self.keep_out_edge()
            }
        } else {
            false
        }
    }

    fn select_graph_meta(&self, graph_meta: &[u8]) -> bool {
        if filter_graph_meta(&self.filter, graph_meta) {
            if is_vertex_graph_meta(graph_meta) {
                return true;
            }

            if is_in_edge_graph_meta(graph_meta) {
                self.in_edge_count.set(self.in_edge_count.get() + 1);
                self.keep_in_edge()
            } else {
                self.out_edge_count.set(self.out_edge_count.get() + 1);
                self.keep_out_edge()
            }
        } else {
            false
        }
    }

    fn drop_all_remaining(&self) -> bool {
        !self.keep_edge()
    }

    fn reset(&self) {
        self.out_edge_count.set(0);
        self.in_edge_count.set(0);
    }
}
