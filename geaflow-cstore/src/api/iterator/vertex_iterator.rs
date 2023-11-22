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
    graph::vertex::Vertex,
    iterator::{
        vertex_scan_iterator::VertexScanIter,
        IteratorType,
        IteratorType::{Empty, Get, Scan},
    },
};

/// VertexIter is an iterator wrapper, inner iterator
/// is vertex vector iterator or vertex scan iterator.
pub struct VertexIter<'a> {
    iter_type: IteratorType,
    vertex_get_iter: Option<IntoIter<Vertex>>,
    vertex_scan_iter: Option<VertexScanIter<'a>>,
}

impl<'a> VertexIter<'a> {
    pub fn from_vec(vertex_vec: Vec<Vertex>) -> Self {
        VertexIter {
            iter_type: Get,
            vertex_get_iter: Some(vertex_vec.into_iter()),
            vertex_scan_iter: None,
        }
    }

    pub fn from_scan_iter(vertex_scan_iter: VertexScanIter<'a>) -> Self {
        VertexIter {
            iter_type: Scan,
            vertex_get_iter: None,
            vertex_scan_iter: Some(vertex_scan_iter),
        }
    }

    pub fn empty_iter() -> Self {
        VertexIter {
            iter_type: Empty,
            vertex_get_iter: None,
            vertex_scan_iter: None,
        }
    }
}

impl<'a> Iterator for VertexIter<'a> {
    type Item = Vertex;

    fn next(&mut self) -> Option<Self::Item> {
        match self.iter_type {
            Empty => None,
            Get => self.vertex_get_iter.as_mut().unwrap().next(),
            Scan => self.vertex_scan_iter.as_mut().unwrap().next(),
        }
    }
}
