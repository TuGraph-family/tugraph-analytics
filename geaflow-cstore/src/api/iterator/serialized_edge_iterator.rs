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

use crate::api::iterator::{edge_iterator::EdgeIter, serializer::serialize_edge};

/// SerializedEdgeIter is EdgeIter wrapper, return serialized edge.
pub struct SerializedEdgeIter<'a> {
    edge_iter: EdgeIter<'a>,
}

impl<'a> SerializedEdgeIter<'a> {
    pub fn from_edge_iter(edge_iter: EdgeIter<'a>) -> Self {
        Self { edge_iter }
    }

    pub fn empty_iter() -> Self {
        Self {
            edge_iter: EdgeIter::empty_iter(),
        }
    }
}

impl<'a> Iterator for SerializedEdgeIter<'a> {
    type Item = Vec<u8>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(edge) = self.edge_iter.next() {
            return Some(serialize_edge(&edge));
        }

        None
    }
}
