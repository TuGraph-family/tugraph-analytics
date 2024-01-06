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

use crate::api::{
    graph::vertex_and_edge::VertexAndEdge,
    iterator::{
        vertex_and_edge_scan_iterator::VertexAndEdgeScanIter,
        IteratorType,
        IteratorType::{Empty, Get, Scan},
    },
};

/// VertexAndEdgeIter is an iterator wrapper, inner iterator
/// is VertexAndEdge vector iterator or VertexAndEdge scan iterator.
pub struct VertexAndEdgeIter<'a> {
    iter_type: IteratorType,
    vertex_and_edge_get_iter: Option<IntoIter<VertexAndEdge<'a>>>,
    vertex_and_edge_scan_iter: Option<VertexAndEdgeScanIter<'a>>,
}

impl<'a> VertexAndEdgeIter<'a> {
    pub fn from_vec(vertex_and_edge_iter_vec: Vec<VertexAndEdge<'a>>) -> Self {
        Self {
            iter_type: Get,
            vertex_and_edge_get_iter: Some(vertex_and_edge_iter_vec.into_iter()),
            vertex_and_edge_scan_iter: None,
        }
    }

    pub fn from_scan_iter(vertex_and_edge_scan_iter: VertexAndEdgeScanIter<'a>) -> Self {
        Self {
            iter_type: Scan,
            vertex_and_edge_get_iter: None,
            vertex_and_edge_scan_iter: Some(vertex_and_edge_scan_iter),
        }
    }

    pub fn empty_iter() -> Self {
        Self {
            iter_type: Empty,
            vertex_and_edge_scan_iter: None,
            vertex_and_edge_get_iter: None,
        }
    }
}

impl<'a> Iterator for VertexAndEdgeIter<'a> {
    type Item = VertexAndEdge<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.iter_type {
            Empty => None,
            Get => self.vertex_and_edge_get_iter.as_mut().unwrap().next(),
            Scan => self.vertex_and_edge_scan_iter.as_mut().unwrap().next(),
        }
    }
}
