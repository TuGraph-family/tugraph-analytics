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
    graph::vertex_and_edge::VertexAndEdge,
    iterator::{
        serialized_edge_iterator::SerializedEdgeIter,
        serialized_vertex_iterator::SerializedVertexIter,
    },
};

/// VertexAndEdge was container, which includes all serialized vertex
/// and edges iterator with same src_id.
pub struct SerializedVertexAndEdge<'a> {
    pub src_id: Vec<u8>,

    pub vertex_iter: SerializedVertexIter<'a>,

    pub edge_iter: SerializedEdgeIter<'a>,
}

impl<'a> SerializedVertexAndEdge<'a> {
    pub fn from_vertex_and_edge(vertex_and_edge: VertexAndEdge<'a>) -> Self {
        Self {
            src_id: vertex_and_edge.src_id,
            vertex_iter: SerializedVertexIter::from_vertex_iter(vertex_and_edge.vertex_iter),
            edge_iter: SerializedEdgeIter::from_edge_iter(vertex_and_edge.edge_iter),
        }
    }
    pub fn empty_vertex_and_edge() -> Self {
        SerializedVertexAndEdge {
            src_id: vec![],
            vertex_iter: SerializedVertexIter::empty_iter(),
            edge_iter: SerializedEdgeIter::empty_iter(),
        }
    }

    pub fn src_id(&self) -> &[u8] {
        self.src_id.as_slice()
    }
}
