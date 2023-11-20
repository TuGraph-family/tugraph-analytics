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

use crate::api::iterator::{serializer::serialize_vertex, vertex_iterator::VertexIter};

/// SerializedVertexIter is VertexIter wrapper, return serialized vertex.
pub struct SerializedVertexIter<'a> {
    vertex_iter: VertexIter<'a>,
}

impl<'a> SerializedVertexIter<'a> {
    pub fn from_vertex_iter(vertex_iter: VertexIter<'a>) -> Self {
        Self { vertex_iter }
    }

    pub fn empty_iter() -> Self {
        Self {
            vertex_iter: VertexIter::empty_iter(),
        }
    }
}

impl<'a> Iterator for SerializedVertexIter<'a> {
    type Item = Vec<u8>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(vertex) = self.vertex_iter.next() {
            return Some(serialize_vertex(&vertex));
        }

        None
    }
}
