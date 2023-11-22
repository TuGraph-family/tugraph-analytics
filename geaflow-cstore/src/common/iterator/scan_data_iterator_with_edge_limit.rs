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
        filter::{filter_graph_info, reset_filter, select_graph_info, StoreFilterPushdown},
        graph::graph_info_util::is_vertex,
    },
    iterator::scan_data_iterator::{MergedGraphData, ScanDataMergeIterator},
};

pub struct ScanDataEdgeLimitedIterator<'a> {
    scan_data_merge_iterator: Peekable<ScanDataMergeIterator<'a>>,
    filter_pushdown: Rc<StoreFilterPushdown>,
    cur_key: u32,
    has_vertex: bool,
}

impl<'a> ScanDataEdgeLimitedIterator<'a> {
    pub fn new(
        scan_data_merge_iterator: ScanDataMergeIterator<'a>,
        filter_pushdown: Rc<StoreFilterPushdown>,
    ) -> Self {
        ScanDataEdgeLimitedIterator {
            scan_data_merge_iterator: scan_data_merge_iterator.peekable(),
            filter_pushdown,
            cur_key: 0,
            has_vertex: false,
        }
    }
}

impl<'a> Iterator for ScanDataEdgeLimitedIterator<'a> {
    type Item = MergedGraphData;

    fn next(&mut self) -> Option<Self::Item> {
        while self.scan_data_merge_iterator.peek().is_some() {
            let next_merged_graph_data = self.scan_data_merge_iterator.next().unwrap();
            if next_merged_graph_data.graph_data.second_key.is_empty() {
                // skip filtered graph data.
                continue;
            }
            if next_merged_graph_data.key != self.cur_key {
                self.has_vertex = false;
                self.cur_key = next_merged_graph_data.key;
                if self.filter_pushdown.filter_context.has_edge_limit {
                    reset_filter(&self.filter_pushdown.filter_handler);
                }
            }

            // keep first vertex.
            if is_vertex(next_merged_graph_data.graph_data.second_key.graph_info) {
                if self.has_vertex {
                    continue;
                } else {
                    self.has_vertex = true;
                }
            }

            if self.filter_pushdown.filter_context.has_edge_limit {
                if select_graph_info(
                    &self.filter_pushdown.filter_handler,
                    next_merged_graph_data.graph_data.second_key.graph_info,
                ) {
                    return Some(next_merged_graph_data);
                }
            } else if filter_graph_info(
                &self.filter_pushdown.filter_handler,
                next_merged_graph_data.graph_data.second_key.graph_info,
            ) {
                return Some(next_merged_graph_data);
            }
        }

        None
    }
}
