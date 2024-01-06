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

use std::vec::IntoIter;

use itertools::Itertools;
use rustc_hash::FxHashMap;

use crate::GraphData;

pub struct SegmentIter<'a> {
    segment_map: &'a FxHashMap<u32, Vec<GraphData>>,
    sorted_key_iter: IntoIter<&'a u32>,
}

impl<'a> SegmentIter<'a> {
    pub fn new(segment_map: &'a FxHashMap<u32, Vec<GraphData>>) -> SegmentIter<'a> {
        SegmentIter {
            sorted_key_iter: segment_map.keys().sorted(),
            segment_map,
        }
    }
}

impl<'a> Iterator for SegmentIter<'a> {
    type Item = (&'a u32, &'a Vec<GraphData>);

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(key) = self.sorted_key_iter.next() {
            let graph_data_vec = self.segment_map.get(key).unwrap();
            return Some((key, graph_data_vec));
        }

        None
    }
}
