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

use std::{
    cmp::Ordering,
    collections::{BinaryHeap, VecDeque},
};

use crate::{
    api::graph::graph_comparator::{cmp_second_key, GraphSortField},
    GraphData,
};

/// Merge and sort graph data according to the order defined in sort field.
pub struct SearchDataIterator<'a> {
    heap: BinaryHeap<ComparableSearchDataVec<'a>>,
    sort_field: &'a [GraphSortField],
}

impl<'a> SearchDataIterator<'a> {
    pub fn new(
        all_graph_data_vec: Vec<VecDeque<GraphData>>,
        sort_field: &'a [GraphSortField],
    ) -> Self {
        let mut heap = BinaryHeap::new();
        for mut graph_data_vec in all_graph_data_vec {
            if !graph_data_vec.is_empty() {
                let head_graph_data = graph_data_vec.pop_front().unwrap();
                heap.push(ComparableSearchDataVec {
                    graph_data_vec,
                    head_graph_data,
                    sort_field,
                });
            }
        }

        Self { heap, sort_field }
    }
}

impl<'a> Iterator for SearchDataIterator<'a> {
    type Item = GraphData;

    fn next(&mut self) -> Option<Self::Item> {
        if self.heap.is_empty() {
            return None;
        }

        let mut comparable_graph_data_vec = self.heap.pop().unwrap();
        if !comparable_graph_data_vec.graph_data_vec.is_empty() {
            let head_graph_data = comparable_graph_data_vec
                .graph_data_vec
                .pop_front()
                .unwrap();
            self.heap.push(ComparableSearchDataVec {
                graph_data_vec: comparable_graph_data_vec.graph_data_vec,
                head_graph_data,
                sort_field: self.sort_field,
            });
        }
        Some(comparable_graph_data_vec.head_graph_data)
    }
}

struct ComparableSearchDataVec<'a> {
    graph_data_vec: VecDeque<GraphData>,
    head_graph_data: GraphData,
    sort_field: &'a [GraphSortField],
}

impl<'a> Eq for ComparableSearchDataVec<'a> {}

impl<'a> PartialEq<Self> for ComparableSearchDataVec<'a> {
    fn eq(&self, other: &Self) -> bool {
        cmp_second_key(
            &self.head_graph_data.second_key,
            &other.head_graph_data.second_key,
            self.sort_field,
        )
        .is_eq()
    }
}

impl<'a> PartialOrd<Self> for ComparableSearchDataVec<'a> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<'a> Ord for ComparableSearchDataVec<'a> {
    fn cmp(&self, other: &Self) -> Ordering {
        cmp_second_key(
            &other.head_graph_data.second_key,
            &self.head_graph_data.second_key,
            self.sort_field,
        )
    }
}
