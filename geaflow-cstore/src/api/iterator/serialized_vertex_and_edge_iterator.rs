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
    graph::serialized_vertex_and_edge::SerializedVertexAndEdge,
    iterator::vertex_and_edge_iterator::VertexAndEdgeIter,
};

/// SerializedVertexAndEdgeIter is VertexAndEdgeIter wrapper, return serialized
/// vertex and edge.
pub struct SerializedVertexAndEdgeIter<'a> {
    vertex_and_edge_iter: VertexAndEdgeIter<'a>,
}

impl<'a> SerializedVertexAndEdgeIter<'a> {
    pub fn from_vertex_and_edge_iter(vertex_and_edge_iter: VertexAndEdgeIter<'a>) -> Self {
        Self {
            vertex_and_edge_iter,
        }
    }

    pub fn empty_iter() -> Self {
        Self {
            vertex_and_edge_iter: VertexAndEdgeIter::empty_iter(),
        }
    }
}

impl<'a> Iterator for SerializedVertexAndEdgeIter<'a> {
    type Item = SerializedVertexAndEdge<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(next_vertex_and_edge) = self.vertex_and_edge_iter.next() {
            return Some(SerializedVertexAndEdge::from_vertex_and_edge(
                next_vertex_and_edge,
            ));
        }

        None
    }
}
