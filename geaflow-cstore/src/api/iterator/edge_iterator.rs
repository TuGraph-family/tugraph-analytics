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
    graph::edge::Edge,
    iterator::{
        edge_scan_iterator::EdgeScanIter,
        IteratorType,
        IteratorType::{Empty, Get, Scan},
    },
};

/// EdgeIter is an iterator wrapper, inner iterator
/// is edge vector iterator or edge scan iterator.
pub struct EdgeIter<'a> {
    iter_type: IteratorType,
    edge_get_iter: Option<IntoIter<Edge>>,
    edge_scan_iter: Option<EdgeScanIter<'a>>,
}

impl<'a> EdgeIter<'a> {
    pub fn from_vec(edge_vec: Vec<Edge>) -> Self {
        Self {
            iter_type: Get,
            edge_get_iter: Some(edge_vec.into_iter()),
            edge_scan_iter: None,
        }
    }

    pub fn from_scan_iter(edge_scan_iter: EdgeScanIter<'a>) -> Self {
        Self {
            iter_type: Scan,
            edge_get_iter: None,
            edge_scan_iter: Some(edge_scan_iter),
        }
    }

    pub fn empty_iter() -> Self {
        Self {
            iter_type: Empty,
            edge_get_iter: None,
            edge_scan_iter: None,
        }
    }
}

impl<'a> Iterator for EdgeIter<'a> {
    type Item = Edge;

    fn next(&mut self) -> Option<Self::Item> {
        match self.iter_type {
            Empty => None,
            Get => self.edge_get_iter.as_mut().unwrap().next(),
            Scan => self.edge_scan_iter.as_mut().unwrap().next(),
        }
    }
}
