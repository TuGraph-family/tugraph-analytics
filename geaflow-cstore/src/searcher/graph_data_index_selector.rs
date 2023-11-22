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

use std::{cmp::Ordering, collections::BinaryHeap};

use crate::{
    api::{
        filter::{drop_all_remaining, select_graph_info, StoreFilterPushdown},
        graph::graph_comparator::{cmp_second_key, GraphSortField},
    },
    GraphDataIndex,
};

/// Merge and sort graph data according to the order defined in sort field.
pub struct GraphDataIndexSelector<'a> {
    heap: BinaryHeap<ComparableGraphDataIndexVec<'a>>,
}

impl<'a> GraphDataIndexSelector<'a> {
    pub fn new(
        graph_data_index_vec: &'a mut [GraphDataIndex],
        sort_field: &'a [GraphSortField],
    ) -> Self {
        let mut heap = BinaryHeap::new();
        for graph_data_index in graph_data_index_vec {
            if !graph_data_index.second_keys.is_empty() {
                graph_data_index.all_filtered = true;
                heap.push(ComparableGraphDataIndexVec {
                    graph_data_index,
                    cur_index: 0,
                    sort_field,
                });
            }
        }

        Self { heap }
    }

    pub fn select(&mut self, filter_pushdown: &StoreFilterPushdown) {
        while !self.heap.is_empty() {
            let comparable_graph_data_index_opt = self.heap.pop();
            let mut comparable_graph_data_index = comparable_graph_data_index_opt.unwrap();
            let head_second_key = comparable_graph_data_index
                .graph_data_index
                .second_keys
                .get_mut(comparable_graph_data_index.cur_index)
                .unwrap();

            if drop_all_remaining(&filter_pushdown.filter_handler)
                && comparable_graph_data_index.graph_data_index.all_filtered
            {
                continue;
            }

            let keep =
                select_graph_info(&filter_pushdown.filter_handler, head_second_key.graph_info);

            if !keep {
                head_second_key.set_empty();
            } else {
                comparable_graph_data_index.graph_data_index.all_filtered = false;
            }

            if comparable_graph_data_index.cur_index
                >= comparable_graph_data_index
                    .graph_data_index
                    .second_keys
                    .len()
                    - 1
            {
                continue;
            }

            comparable_graph_data_index.cur_index += 1;

            self.heap.push(comparable_graph_data_index);
        }
    }
}

struct ComparableGraphDataIndexVec<'a> {
    graph_data_index: &'a mut GraphDataIndex,
    cur_index: usize,
    sort_field: &'a [GraphSortField],
}

impl<'a> Eq for ComparableGraphDataIndexVec<'a> {}

impl<'a> PartialEq<Self> for ComparableGraphDataIndexVec<'a> {
    fn eq(&self, other: &Self) -> bool {
        cmp_second_key(
            self.graph_data_index
                .second_keys
                .get(self.cur_index)
                .unwrap(),
            other
                .graph_data_index
                .second_keys
                .get(other.cur_index)
                .unwrap(),
            self.sort_field,
        )
        .is_eq()
    }
}

impl<'a> PartialOrd<Self> for ComparableGraphDataIndexVec<'a> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<'a> Ord for ComparableGraphDataIndexVec<'a> {
    fn cmp(&self, other: &Self) -> Ordering {
        cmp_second_key(
            other
                .graph_data_index
                .second_keys
                .get(other.cur_index)
                .unwrap(),
            self.graph_data_index
                .second_keys
                .get(self.cur_index)
                .unwrap(),
            self.sort_field,
        )
    }
}
