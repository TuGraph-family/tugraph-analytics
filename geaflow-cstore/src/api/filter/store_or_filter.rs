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

pub struct StoreOrFilter {
    filters: Vec<FilterHandler>,
}

impl StoreOrFilter {
    pub fn new(filters: Vec<FilterHandler>) -> Self {
        StoreOrFilter { filters }
    }

    pub fn get_or_filter(&self) -> &[FilterHandler] {
        self.filters.as_slice()
    }
}

impl IFilter for StoreOrFilter {
    fn filter_graph_info(&self, graph_info: u64) -> bool {
        for filter in self.filters.iter() {
            if filter_graph_info(filter, graph_info) {
                return true;
            }
        }
        false
    }

    fn filter_graph_meta(&self, graph_meta: &[u8]) -> bool {
        for filter in self.filters.iter() {
            if filter_graph_meta(filter, graph_meta) {
                return true;
            }
        }
        false
    }
}
