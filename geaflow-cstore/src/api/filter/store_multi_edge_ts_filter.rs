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
        get_event_time, get_event_time_from_graph_meta, is_vertex, is_vertex_graph_meta,
    },
};

pub struct StoreMultiEdgeTsFilter {
    time_range_list: Vec<(i64, i64)>,
}

impl StoreMultiEdgeTsFilter {
    pub fn new(time_range_list: Vec<(i64, i64)>) -> Self {
        StoreMultiEdgeTsFilter { time_range_list }
    }
}

impl IFilter for StoreMultiEdgeTsFilter {
    fn filter_graph_info(&self, graph_info: u64) -> bool {
        if is_vertex(graph_info) {
            return true;
        }

        let ts = get_event_time(graph_info) as i64;
        for time_range in self.time_range_list.iter() {
            if ts >= time_range.0 && ts < time_range.1 {
                return true;
            }
        }
        false
    }

    fn filter_graph_meta(&self, graph_meta: &[u8]) -> bool {
        if is_vertex_graph_meta(graph_meta) {
            return true;
        }

        let ts = get_event_time_from_graph_meta(graph_meta) as i64;
        for time_range in self.time_range_list.iter() {
            if ts >= time_range.0 && ts < time_range.1 {
                return true;
            }
        }
        false
    }
}
