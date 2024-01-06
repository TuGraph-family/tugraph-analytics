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

use crate::api::filter::{filter_graph_info, filter_graph_meta, FilterHandler, IFilter};

pub struct StoreAndFilter {
    filters: Vec<FilterHandler>,
}

impl StoreAndFilter {
    pub fn new(filters: Vec<FilterHandler>) -> Self {
        StoreAndFilter { filters }
    }
}

impl IFilter for StoreAndFilter {
    fn filter_graph_info(&self, graph_info: u64) -> bool {
        for filter in self.filters.iter() {
            if !filter_graph_info(filter, graph_info) {
                return false;
            }
        }
        true
    }

    fn filter_graph_meta(&self, graph_meta: &[u8]) -> bool {
        for filter in self.filters.iter() {
            if !filter_graph_meta(filter, graph_meta) {
                return false;
            }
        }
        true
    }
}
