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
    graph::graph_info_util::{
        get_label, get_label_from_graph_meta, is_vertex, is_vertex_graph_meta,
    },
};

pub struct StoreEdgeLabelFilter {
    expected_labels: Vec<u16>,
}

impl StoreEdgeLabelFilter {
    pub fn new(labels: Vec<u16>) -> Self {
        StoreEdgeLabelFilter {
            expected_labels: labels,
        }
    }
}

impl IFilter for StoreEdgeLabelFilter {
    fn filter_graph_info(&self, graph_info: u64) -> bool {
        if is_vertex(graph_info) {
            return true;
        }

        let label = get_label(graph_info);
        for expect_label in self.expected_labels.iter() {
            if expect_label.eq(&label) {
                return true;
            }
        }
        false
    }

    fn filter_graph_meta(&self, graph_meta: &[u8]) -> bool {
        if is_vertex_graph_meta(graph_meta) {
            return true;
        }

        let label = get_label_from_graph_meta(graph_meta);
        for expect_label in self.expected_labels.iter() {
            if expect_label.eq(&label) {
                return true;
            }
        }
        false
    }
}
