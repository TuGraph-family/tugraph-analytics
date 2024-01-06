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

use crate::api::iterator::{edge_iterator::EdgeIter, vertex_iterator::VertexIter};

/// VertexAndEdge was container, which includes all vertex and edges with same
/// src_id.
pub struct VertexAndEdge<'a> {
    pub src_id: Vec<u8>,

    pub vertex_iter: VertexIter<'a>,

    pub edge_iter: EdgeIter<'a>,
}

impl<'a> VertexAndEdge<'a> {
    pub fn empty_vertex_and_edge() -> Self {
        VertexAndEdge {
            src_id: vec![],
            vertex_iter: VertexIter::from_vec(vec![]),
            edge_iter: EdgeIter::from_vec(vec![]),
        }
    }

    pub fn src_id(&self) -> &[u8] {
        self.src_id.as_slice()
    }
}
