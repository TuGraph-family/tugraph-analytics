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

use crate::{
    api::graph::{
        edge::Edge,
        graph_info_util::{
            get_event_time, get_label, is_in_edge, HAS_VALUE_MASK, IN_EDGE_MASK, RIGHT_LABEL_POS,
            TIME_MASK, VERTEX_MASK,
        },
        vertex::Vertex,
        EdgeDirection,
    },
    convert_u64_to_u32,
    engine::dict::label_dict::LabelDict,
    util::time_util::time_now_as_secs,
    GraphData, SecondKey, EMPTY_SECOND_KEY_SEQUENCE_ID,
};

#[derive(Default)]
pub struct GraphSerde {}

impl GraphSerde {
    pub fn serialize_edge(&self, edge: &Edge, label_dict: &LabelDict) -> GraphData {
        let second_key = SecondKey {
            ts: self.get_write_time(),
            graph_info: self.serialize_edge_graph_info(edge, label_dict),
            target_id: Vec::from(edge.target_id()),
            sequence_id: EMPTY_SECOND_KEY_SEQUENCE_ID,
        };
        GraphData {
            second_key,
            property: Vec::from(edge.property()),
        }
    }

    pub fn deserialize_edge(
        &self,
        key: &[u8],
        graph_data: &GraphData,
        label_dict: &LabelDict,
    ) -> Edge {
        let graph_info = graph_data.second_key.graph_info;
        let label_type = get_label(graph_info);
        let label_option = label_dict.get_label(label_type);
        assert!(label_option.is_some());

        Edge::create_id_time_label_edge(
            Vec::from(key),
            graph_data.second_key.target_id.clone(),
            get_event_time(graph_data.second_key.graph_info),
            label_option.unwrap(),
            if is_in_edge(graph_data.second_key.graph_info) {
                EdgeDirection::In
            } else {
                EdgeDirection::Out
            },
            graph_data.property.clone(),
        )
    }

    pub fn serialize_vertex(&self, vertex: &Vertex, label_dict: &LabelDict) -> GraphData {
        let second_key = SecondKey {
            ts: self.get_write_time(),
            graph_info: self.serialize_vertex_graph_info(vertex, label_dict),
            target_id: Vec::from(vertex.src_id()),
            sequence_id: EMPTY_SECOND_KEY_SEQUENCE_ID,
        };
        GraphData {
            second_key,
            property: Vec::from(vertex.property()),
        }
    }

    pub fn deserialize_vertex_in_scan(
        &self,
        graph_data: &GraphData,
        label_dict: &LabelDict,
    ) -> Vertex {
        let graph_info = graph_data.second_key.graph_info;
        let label_type = get_label(graph_info);
        let label_option = label_dict.get_label(label_type);
        assert!(label_option.is_some());

        Vertex::create_id_time_label_vertex(
            graph_data.second_key.target_id.clone(),
            get_event_time(graph_info),
            label_option.unwrap().clone(),
            graph_data.property.clone(),
        )
    }

    pub fn deserialize_vertex(
        &self,
        key: &[u8],
        graph_data: &GraphData,
        label_dict: &LabelDict,
    ) -> Vertex {
        let graph_info = graph_data.second_key.graph_info;
        let label_type = get_label(graph_info);
        let label_option = label_dict.get_label(label_type);
        assert!(label_option.is_some());

        Vertex::create_id_time_label_vertex(
            Vec::from(key),
            get_event_time(graph_info),
            label_option.unwrap(),
            graph_data.property.clone(),
        )
    }

    fn serialize_edge_graph_info(&self, edge: &Edge, label_dict: &LabelDict) -> u64 {
        // encode ts.
        let time = edge.ts();
        assert!(time < TIME_MASK);
        let mut other_info = time;

        // encode label.
        // FIXME: change namespace to be configurable.
        let label_type = label_dict.register(edge.label());
        other_info |= (label_type as u64) << RIGHT_LABEL_POS;

        // encode direction.
        if *edge.direction() == EdgeDirection::In {
            other_info |= IN_EDGE_MASK;
        }

        // encode has property.
        if !edge.property().is_empty() {
            other_info |= HAS_VALUE_MASK;
        }

        other_info
    }

    fn serialize_vertex_graph_info(&self, vertex: &Vertex, label_dict: &LabelDict) -> u64 {
        let time = vertex.ts();
        assert!(time < TIME_MASK);
        let mut other_info = time;

        // encode label.
        let label_type = label_dict.register(vertex.label());
        other_info |= (label_type as u64) << RIGHT_LABEL_POS;

        // encode is_vertex mask.
        other_info |= VERTEX_MASK;

        // encode has property.
        if !vertex.property().is_empty() {
            other_info |= HAS_VALUE_MASK;
        }

        other_info
    }

    fn get_write_time(&self) -> u32 {
        convert_u64_to_u32!(time_now_as_secs())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use bytes::BufMut;

    use crate::{
        api::graph::{edge::Edge, graph_serde::GraphSerde, vertex::Vertex, EdgeDirection},
        engine::dict::label_dict::LabelDict,
        SIZE_OF_U32,
    };

    #[test]
    fn test_vertex_serde() {
        let graph_serde = GraphSerde::default();
        let label_dict = Arc::new(LabelDict::default());
        let mut src_id_bytes: Vec<u8> = Vec::with_capacity(SIZE_OF_U32);
        for i in 0..100 {
            src_id_bytes.put_u32(i);
            let vertex = Vertex::create_id_time_label_vertex(
                src_id_bytes.clone(),
                i as u64,
                String::from("test"),
                src_id_bytes.clone(),
            );
            let graph_data = graph_serde.serialize_vertex(&vertex, &label_dict);
            let deserialized_vertex =
                graph_serde.deserialize_vertex(&src_id_bytes, &graph_data, &label_dict);

            assert_eq!(vertex.src_id(), deserialized_vertex.src_id());
            assert_eq!(vertex.ts(), deserialized_vertex.ts());
            assert_eq!(vertex.property(), deserialized_vertex.property());
            assert_eq!(vertex.label(), deserialized_vertex.label());
        }
    }

    #[test]
    fn test_edge_serde() {
        let graph_serde = GraphSerde::default();
        let label_dict = Arc::new(LabelDict::default());
        let mut src_id_bytes: Vec<u8> = Vec::with_capacity(SIZE_OF_U32);
        for i in 0..100 {
            src_id_bytes.put_u32(i);

            let mut target_id_bytes: Vec<u8> = Vec::with_capacity(SIZE_OF_U32);
            target_id_bytes.put_u32(i + 1);

            let edge = Edge::create_id_time_label_edge(
                src_id_bytes.clone(),
                target_id_bytes.clone(),
                i as u64,
                String::from("test"),
                EdgeDirection::Out,
                src_id_bytes.clone(),
            );

            let graph_data = graph_serde.serialize_edge(&edge, &label_dict);
            let deserialized_edge =
                graph_serde.deserialize_edge(&src_id_bytes, &graph_data, &label_dict);

            assert_eq!(edge.src_id(), deserialized_edge.src_id());
            assert_eq!(edge.target_id(), deserialized_edge.target_id());
            assert_eq!(edge.direction(), deserialized_edge.direction());
            assert_eq!(edge.ts(), deserialized_edge.ts());
            assert_eq!(edge.property(), deserialized_edge.property());
            assert_eq!(edge.label(), deserialized_edge.label());
        }
    }
}
