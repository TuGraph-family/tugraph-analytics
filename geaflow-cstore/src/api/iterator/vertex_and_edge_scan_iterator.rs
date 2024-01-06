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

use std::{iter::Peekable, rc::Rc};

use crate::{
    api::{
        filter::StoreFilterPushdown,
        graph::{
            edge::Edge, graph_info_util::is_vertex, graph_serde::GraphSerde, vertex::Vertex,
            vertex_and_edge::VertexAndEdge,
        },
        iterator::{edge_iterator::EdgeIter, vertex_iterator::VertexIter},
    },
    engine::dict::id_dict::IdDict,
    iterator::{
        scan_data_iterator::MergedGraphData,
        scan_data_iterator_with_edge_limit::ScanDataEdgeLimitedIterator,
    },
    Engine,
};

/// Return VertexAndEdgeScanIter by calling scan interface.
pub struct VertexAndEdgeScanIter<'a> {
    graph_serde: &'a GraphSerde,
    engine: &'a Engine,
    scan_data_iterator: Peekable<ScanDataEdgeLimitedIterator<'a>>,
    filter_pushdown: Rc<StoreFilterPushdown>,
}

impl<'a> VertexAndEdgeScanIter<'a> {
    pub fn new(
        graph_serde: &'a GraphSerde,
        engine: &'a Engine,
        filter_pushdown: StoreFilterPushdown,
    ) -> Self {
        let filter_pushdown_rc = Rc::new(filter_pushdown);
        Self {
            graph_serde,
            engine,
            scan_data_iterator: engine.scan(Rc::clone(&filter_pushdown_rc)),
            filter_pushdown: filter_pushdown_rc,
        }
    }

    fn get_next_vertex(&mut self) -> Option<(u32, Vertex)> {
        while self.scan_data_iterator.peek().is_some() {
            let mut first_merged_graph_data = self.scan_data_iterator.next().unwrap();
            let first_key = first_merged_graph_data.key;

            // if vertex exist, the first graph_data should be vertex, or skip all the
            // edge.
            if !is_vertex(first_merged_graph_data.graph_data.second_key.graph_info) {
                while self.scan_data_iterator.peek().is_some()
                    && self.scan_data_iterator.peek().unwrap().key == first_key
                {
                    // skip all the edge.
                    self.scan_data_iterator.next();
                }
                continue;
            }

            if self
                .filter_pushdown
                .filter_context
                .drop_vertex_value_property
            {
                first_merged_graph_data.graph_data.property = vec![];
            }
            let first_vertex = self.graph_serde.deserialize_vertex_in_scan(
                &first_merged_graph_data.graph_data,
                &self.engine.label_dict,
            );

            return Some((first_key, first_vertex));
        }

        None
    }

    fn get_next_vertex_and_edge_if_must_contains_vertex(&mut self) -> Option<VertexAndEdge<'a>> {
        let mut vertex_vec = vec![];
        let mut edge_vec = vec![];

        // if there's no vertex, return None and terminate the iterator.
        let next_vertex = self.get_next_vertex();
        next_vertex.as_ref()?;

        let key_and_vertex = next_vertex.unwrap();

        vertex_vec.push(key_and_vertex.1);
        let src_id = vertex_vec.first().unwrap().src_id();

        while self.scan_data_iterator.peek().is_some()
            && self.scan_data_iterator.peek().unwrap().key == key_and_vertex.0
        {
            let mut next_merged_graph_data = self.scan_data_iterator.next().unwrap();
            // keep first vertex and skip others.
            if is_vertex(next_merged_graph_data.graph_data.second_key.graph_info) {
                continue;
            }

            if self.filter_pushdown.filter_context.drop_edge_value_property {
                next_merged_graph_data.graph_data.property = vec![];
            }

            self.deserialize_next_edge(src_id, next_merged_graph_data, &mut edge_vec);
        }

        Some(VertexAndEdge {
            src_id: Vec::from(src_id),
            vertex_iter: VertexIter::from_vec(vertex_vec),
            edge_iter: EdgeIter::from_vec(edge_vec),
        })
    }

    fn get_next_vertex_and_edge(&mut self) -> Option<VertexAndEdge<'a>> {
        let mut vertex_vec = vec![];
        let mut edge_vec = vec![];

        self.scan_data_iterator.peek()?;

        let mut has_vertex = false;
        let first_key = self.scan_data_iterator.peek().unwrap().key;
        let src_id = self.engine.id_dict.get_origin_key(first_key).unwrap();
        while self.scan_data_iterator.peek().is_some()
            && self.scan_data_iterator.peek().unwrap().key == first_key
        {
            let mut next_merged_graph_data = self.scan_data_iterator.next().unwrap();
            if is_vertex(next_merged_graph_data.graph_data.second_key.graph_info) {
                if has_vertex {
                    continue;
                } else {
                    if self
                        .filter_pushdown
                        .filter_context
                        .drop_vertex_value_property
                    {
                        next_merged_graph_data.graph_data.property = vec![];
                    }
                    has_vertex = true;
                    self.deserialize_next_vertex(
                        src_id.as_slice(),
                        next_merged_graph_data,
                        &mut vertex_vec,
                    );
                }
            } else {
                if self.filter_pushdown.filter_context.drop_edge_value_property {
                    next_merged_graph_data.graph_data.property = vec![];
                }

                self.deserialize_next_edge(
                    src_id.as_slice(),
                    next_merged_graph_data,
                    &mut edge_vec,
                );
            }
        }

        Some(VertexAndEdge {
            src_id,
            vertex_iter: VertexIter::from_vec(vertex_vec),
            edge_iter: EdgeIter::from_vec(edge_vec),
        })
    }

    fn deserialize_next_edge(
        &self,
        src_id: &[u8],
        merged_graph_data: MergedGraphData,
        edge_vec: &mut Vec<Edge>,
    ) {
        edge_vec.push(self.graph_serde.deserialize_edge(
            src_id,
            &merged_graph_data.graph_data,
            &self.engine.label_dict,
        ));
    }

    fn deserialize_next_vertex(
        &self,
        src_id: &[u8],
        merged_graph_data: MergedGraphData,
        vertex_vec: &mut Vec<Vertex>,
    ) {
        vertex_vec.push(self.graph_serde.deserialize_vertex(
            src_id,
            &merged_graph_data.graph_data,
            &self.engine.label_dict,
        ));
    }
}

// TODO: optimize with streaming iterator.
impl<'a> Iterator for VertexAndEdgeScanIter<'a> {
    type Item = VertexAndEdge<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.scan_data_iterator.peek().is_some() {
            return if self.filter_pushdown.filter_context.vertex_must_contains {
                self.get_next_vertex_and_edge_if_must_contains_vertex()
            } else {
                self.get_next_vertex_and_edge()
            };
        }

        None
    }
}
