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

use std::time::Instant;

use rustc_hash::FxHashMap;

use crate::{
    api::graph::graph_comparator::cmp_second_key,
    common::{GraphData, KEY_SIZE},
    config::StoreConfig,
    log_util::trace,
    metric, EngineContext,
};

pub struct GraphSegment {
    data_map: FxHashMap<u32, Vec<GraphData>>,
    data_size: usize,
    mem_segment_size: usize,
}

impl GraphSegment {
    pub fn new(store_config: &StoreConfig) -> Self {
        GraphSegment {
            data_map: FxHashMap::default(),
            data_size: 0,
            mem_segment_size: store_config.mem_segment_size,
        }
    }

    /// Put serialized vertex or edge to hash map.
    /// All vertex or edge with same ID was appended to one list.
    pub fn put(&mut self, key: u32, value: GraphData) {
        let entry_size = KEY_SIZE + value.property.len();
        metric::GRAPH_SEGMENT_MEMORY.increment(entry_size as f64);

        self.data_size += entry_size;
        // Recently written data should be read earlier.
        self.data_map.entry(key).or_default().push(value);
    }

    /// Search serialized vertex or edge from segment.
    pub fn get(&self, key: u32, engine_context: &EngineContext) -> Option<Vec<GraphData>> {
        let graph_data_vec_opt = self.data_map.get(&key);
        graph_data_vec_opt?;

        // sort graph_data by comparator.
        let sort_field = engine_context.sort_field.as_slice();
        let mut graph_data_vec = graph_data_vec_opt.unwrap().clone();
        graph_data_vec.sort_by(|graph_data_1, graph_data_2| {
            cmp_second_key(
                &graph_data_1.second_key,
                &graph_data_2.second_key,
                sort_field,
            )
        });

        Some(graph_data_vec)
    }

    /// Return true if need freeze graph data to file.
    pub fn need_freeze(&self) -> bool {
        self.data_size >= self.mem_segment_size
    }

    /// Return true if no graph data in segment.
    pub fn is_empty(&self) -> bool {
        self.data_map.is_empty()
    }

    /// Return static graph data iterator.
    pub fn view(&mut self, engine_context: &EngineContext) -> &FxHashMap<u32, Vec<GraphData>> {
        // sort graph_data by comparator.
        let start = Instant::now();
        let sort_field = engine_context.sort_field.as_slice();
        for entry in self.data_map.iter_mut() {
            entry.1.sort_by(|graph_data_1, graph_data_2| {
                cmp_second_key(
                    &graph_data_1.second_key,
                    &graph_data_2.second_key,
                    sort_field,
                )
            });
        }
        trace!(
            "sort graph_data cost {}us",
            (Instant::now() - start).as_micros()
        );

        // return sorted view.
        &self.data_map
    }

    /// Release memory in segment.
    pub fn close(&mut self) {
        metric::GRAPH_SEGMENT_MEMORY.decrement(self.data_size as f64);
        self.data_map.clear();
        self.data_size = 0;
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use rand::Rng;

    use crate::{
        api::graph::{edge::Edge, graph_serde::GraphSerde, EdgeDirection},
        engine::dict::label_dict::LabelDict,
        log_util::{self, LogLevel, LogType},
        CStoreConfig, EngineContext, GraphSegment,
    };

    #[test]
    fn test_segment() {
        log_util::try_init(LogType::ConsoleAndFile, LogLevel::Debug, 0);
        let mut rng = rand::thread_rng();

        let cstore_config = CStoreConfig::default();

        let graph_serde = GraphSerde::default();

        let label_dict = Arc::new(LabelDict::default());
        let engine_context = EngineContext::new(&label_dict, &cstore_config);

        let mut graph_segment = GraphSegment::new(&cstore_config.store);
        for i in 0..10000u32 {
            let edge = Edge::create_id_time_label_edge(
                1_i32.to_be_bytes().to_vec(),
                i.to_be_bytes().to_vec(),
                rng.gen::<u32>() as u64,
                String::from("test"),
                if i % 2 == 0 {
                    EdgeDirection::In
                } else {
                    EdgeDirection::Out
                },
                i.to_be_bytes().to_vec(),
            );
            graph_segment.put(1, graph_serde.serialize_edge(&edge, &label_dict));
        }
        let data_map = graph_segment.view(&engine_context);

        let key = 1_i32.to_be_bytes().to_vec();
        for entry in data_map.iter() {
            let mut previous_edge: Option<Edge> = None;
            for graph_data in entry.1 {
                let edge = graph_serde.deserialize_edge(key.as_slice(), graph_data, &label_dict);
                if previous_edge.is_some() {
                    assert!(edge.direction() <= previous_edge.as_ref().unwrap().direction());
                    if edge.direction() == previous_edge.as_ref().unwrap().direction() {
                        assert!(edge.ts() <= previous_edge.as_ref().unwrap().ts());
                    }
                }
                previous_edge = Some(edge);
            }
        }

        graph_segment.close();
    }
}
