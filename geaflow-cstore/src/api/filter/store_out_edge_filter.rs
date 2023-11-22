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

use crate::api::{
    filter::IFilter,
    graph::graph_info_util::{is_in_edge, is_in_edge_graph_meta, is_vertex, is_vertex_graph_meta},
};

#[derive(Default)]
pub struct StoreOutEdgeFilter {}

impl IFilter for StoreOutEdgeFilter {
    fn filter_graph_info(&self, graph_info: u64) -> bool {
        is_vertex(graph_info) || !is_in_edge(graph_info)
    }

    fn filter_graph_meta(&self, graph_meta: &[u8]) -> bool {
        is_vertex_graph_meta(graph_meta) || !is_in_edge_graph_meta(graph_meta)
    }
}
