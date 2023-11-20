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
    api::{
        filter::{
            converter::{
                convert_to_filter_pushdown, convert_to_filter_pushdown_map, decode_pushdown,
            },
            create_empty_filter_pushdown, StoreFilterPushdown,
        },
        graph::{
            edge::Edge, graph_info_util::is_vertex, graph_serde::GraphSerde,
            serialized_vertex_and_edge::SerializedVertexAndEdge, vertex::Vertex,
            vertex_and_edge::VertexAndEdge,
        },
        iterator::{
            edge_iterator::EdgeIter, edge_scan_iterator::EdgeScanIter,
            serialized_edge_iterator::SerializedEdgeIter,
            serialized_vertex_and_edge_iterator::SerializedVertexAndEdgeIter,
            serialized_vertex_iterator::SerializedVertexIter,
            vertex_and_edge_iterator::VertexAndEdgeIter,
            vertex_and_edge_scan_iterator::VertexAndEdgeScanIter, vertex_iterator::VertexIter,
            vertex_scan_iterator::VertexScanIter,
        },
    },
    engine::store_engine::ActionTrait,
    CStoreConfig, Engine,
};

pub struct CStore {
    graph_serde: GraphSerde,

    engine: Engine,
}

impl CStore {
    /// Create new CStore instance.
    pub fn new(config: CStoreConfig) -> Self {
        CStore {
            graph_serde: GraphSerde::default(),
            engine: Engine::new(config),
        }
    }

    /// Add vertex to graph engine.
    pub fn add_vertex(&mut self, vertex: Vertex) {
        let graph_data = self
            .graph_serde
            .serialize_vertex(&vertex, &self.engine.label_dict);
        let id = self.engine.register(vertex.src_id());
        self.engine.put(id, graph_data);
    }

    /// Add edge to graph engine.
    pub fn add_edge(&mut self, edge: Edge) {
        let graph_data = self
            .graph_serde
            .serialize_edge(&edge, &self.engine.label_dict);
        let id = self.engine.register(edge.src_id());
        self.engine.put(id, graph_data);
    }

    /// get vertex from graph engine according to the src_id.
    pub fn get_vertex(&self, src_id: &[u8]) -> VertexIter {
        self.get_vertex_with_filter_pushdown(src_id, &create_empty_filter_pushdown())
    }

    /// get serialized vertex from graph engine according to the src_id.
    pub fn get_serialized_vertex(&self, src_id: &[u8]) -> SerializedVertexIter {
        SerializedVertexIter::from_vertex_iter(
            self.get_vertex_with_filter_pushdown(src_id, &create_empty_filter_pushdown()),
        )
    }

    /// get vertex from graph engine according to the src_id and filter pushdown
    /// buf.
    pub fn get_vertex_with_filter_pushdown_buf(
        &self,
        src_id: &[u8],
        filter_pushdown_buf: &[u8],
    ) -> VertexIter {
        // decode push down.
        let pushdown = decode_pushdown(filter_pushdown_buf);
        let filter_pushdown = convert_to_filter_pushdown(&pushdown, &self.engine.label_dict);

        self.get_vertex_with_filter_pushdown(src_id, &filter_pushdown)
    }

    /// get serialized vertex from graph engine according to the src_id and
    /// filter pushdown buf.
    pub fn get_serialized_vertex_with_filter_pushdown_buf(
        &self,
        src_id: &[u8],
        filter_pushdown_buf: &[u8],
    ) -> SerializedVertexIter {
        SerializedVertexIter::from_vertex_iter(
            self.get_vertex_with_filter_pushdown_buf(src_id, filter_pushdown_buf),
        )
    }

    /// get vertex from graph engine according to the src_id and filter
    /// pushdown.
    pub fn get_vertex_with_filter_pushdown(
        &self,
        src_id: &[u8],
        filter_pushdown: &StoreFilterPushdown,
    ) -> VertexIter {
        if let Some(key) = self.engine.get_id(src_id) {
            if let Some(graph_data_vec) = self.engine.get(key, filter_pushdown) {
                let mut vertex_vec = vec![];
                for graph_data in graph_data_vec {
                    if is_vertex(graph_data.second_key.graph_info) {
                        let vertex = self.graph_serde.deserialize_vertex(
                            src_id,
                            &graph_data,
                            &self.engine.label_dict,
                        );

                        vertex_vec.push(vertex);
                    }
                }
                return VertexIter::from_vec(vertex_vec);
            }
        }
        VertexIter::empty_iter()
    }

    /// get serialized vertex from graph engine according to the src_id and
    /// filter pushdown.
    pub fn get_serialized_vertex_with_filter_pushdown(
        &self,
        src_id: &[u8],
        filter_pushdown: &StoreFilterPushdown,
    ) -> SerializedVertexIter {
        SerializedVertexIter::from_vertex_iter(
            self.get_vertex_with_filter_pushdown(src_id, filter_pushdown),
        )
    }

    /// get edge from graph engine according to the src_id.
    pub fn get_edge(&self, src_id: &[u8]) -> EdgeIter {
        self.get_edge_with_filter_pushdown(src_id, &create_empty_filter_pushdown())
    }

    /// get serialized edge from graph engine according to the src_id.
    pub fn get_serialized_edge(&self, src_id: &[u8]) -> SerializedEdgeIter {
        SerializedEdgeIter::from_edge_iter(
            self.get_edge_with_filter_pushdown(src_id, &create_empty_filter_pushdown()),
        )
    }

    /// get edge from graph engine according to the src_id and filter pushdown
    /// buf.
    pub fn get_edge_with_filter_pushdown_buf(
        &self,
        src_id: &[u8],
        filter_pushdown_buf: &[u8],
    ) -> EdgeIter {
        // decode push down.
        let pushdown = decode_pushdown(filter_pushdown_buf);
        let filter_pushdown = convert_to_filter_pushdown(&pushdown, &self.engine.label_dict);

        self.get_edge_with_filter_pushdown(src_id, &filter_pushdown)
    }

    /// get serialized edge from graph engine according to the src_id and filter
    /// pushdown buf.
    pub fn get_serialized_edge_with_filter_pushdown_buf(
        &self,
        src_id: &[u8],
        filter_pushdown_buf: &[u8],
    ) -> SerializedEdgeIter {
        SerializedEdgeIter::from_edge_iter(
            self.get_edge_with_filter_pushdown_buf(src_id, filter_pushdown_buf),
        )
    }

    /// get edge from graph engine according to the src_id and filter
    /// pushdown.
    pub fn get_edge_with_filter_pushdown(
        &self,
        src_id: &[u8],
        filter_pushdown: &StoreFilterPushdown,
    ) -> EdgeIter {
        if let Some(key) = self.engine.get_id(src_id) {
            if let Some(graph_data_vec) = self.engine.get(key, filter_pushdown) {
                let mut edge_vec = vec![];
                for graph_data in graph_data_vec {
                    if !is_vertex(graph_data.second_key.graph_info) {
                        let edge = self.graph_serde.deserialize_edge(
                            src_id,
                            &graph_data,
                            &self.engine.label_dict,
                        );

                        edge_vec.push(edge);
                    }
                }
                return EdgeIter::from_vec(edge_vec);
            }
        }
        EdgeIter::empty_iter()
    }

    /// get serialized edge from graph engine according to the src_id and
    /// filter pushdown.
    pub fn get_serialized_edge_with_filter_pushdown(
        &self,
        src_id: &[u8],
        filter_pushdown: &StoreFilterPushdown,
    ) -> SerializedEdgeIter {
        SerializedEdgeIter::from_edge_iter(
            self.get_edge_with_filter_pushdown(src_id, filter_pushdown),
        )
    }

    /// get vertex and edge from graph engine according to the src_id.
    pub fn get_vertex_and_edge(&self, src_id: &[u8]) -> VertexAndEdge {
        self.get_vertex_and_edge_with_filter_pushdown(src_id, &create_empty_filter_pushdown())
    }

    /// get serialized vertex and edge from graph engine according to the
    /// src_id.
    pub fn get_serialized_vertex_and_edge(&self, src_id: &[u8]) -> SerializedVertexAndEdge {
        SerializedVertexAndEdge::from_vertex_and_edge(
            self.get_vertex_and_edge_with_filter_pushdown(src_id, &create_empty_filter_pushdown()),
        )
    }

    /// Get vertex and edge from graph engine according to the src_id and
    /// filter pushdown buffer.
    pub fn get_vertex_and_edge_with_filter_pushdown_buf(
        &self,
        src_id: &[u8],
        filter_pushdown_buf: &[u8],
    ) -> VertexAndEdge {
        // decode push down.
        let pushdown = decode_pushdown(filter_pushdown_buf);
        let filter_pushdown = convert_to_filter_pushdown(&pushdown, &self.engine.label_dict);

        self.get_vertex_and_edge_with_filter_pushdown(src_id, &filter_pushdown)
    }

    /// Get serialized vertex and edge from graph engine according to the src_id
    /// and filter pushdown buffer.
    pub fn get_serialized_vertex_and_edge_with_filter_pushdown_buf(
        &self,
        src_id: &[u8],
        filter_pushdown_buf: &[u8],
    ) -> SerializedVertexAndEdge {
        SerializedVertexAndEdge::from_vertex_and_edge(
            self.get_vertex_and_edge_with_filter_pushdown_buf(src_id, filter_pushdown_buf),
        )
    }

    /// Get vertex and edge from graph engine according to the src_id and
    /// filter pushdown.
    pub fn get_vertex_and_edge_with_filter_pushdown(
        &self,
        src_id: &[u8],
        filter_pushdown: &StoreFilterPushdown,
    ) -> VertexAndEdge {
        if let Some(key) = self.engine.get_id(src_id) {
            if let Some(graph_data_vec) = self.engine.get(key, filter_pushdown) {
                let mut vertex_vec = vec![];
                let mut edge_vec = vec![];
                for graph_data in graph_data_vec {
                    if is_vertex(graph_data.second_key.graph_info) {
                        let vertex = self.graph_serde.deserialize_vertex(
                            src_id,
                            &graph_data,
                            &self.engine.label_dict,
                        );
                        vertex_vec.push(vertex);
                    } else {
                        let edge = self.graph_serde.deserialize_edge(
                            src_id,
                            &graph_data,
                            &self.engine.label_dict,
                        );
                        edge_vec.push(edge);
                    }
                }
                return VertexAndEdge {
                    src_id: Vec::from(src_id),
                    vertex_iter: VertexIter::from_vec(vertex_vec),
                    edge_iter: EdgeIter::from_vec(edge_vec),
                };
            }
        }
        VertexAndEdge::empty_vertex_and_edge()
    }

    /// Get serialized vertex and edge from graph engine according to the src_id
    /// and filter pushdown.
    pub fn get_serialized_vertex_and_edge_with_filter_pushdown(
        &self,
        src_id: &[u8],
        filter_pushdown: &StoreFilterPushdown,
    ) -> SerializedVertexAndEdge {
        SerializedVertexAndEdge::from_vertex_and_edge(
            self.get_vertex_and_edge_with_filter_pushdown(src_id, filter_pushdown),
        )
    }

    /// Get vertex iterator according to the src_ids and filter pushdown buffer.
    pub fn get_multi_vertex_with_filter_pushdown_buf(
        &self,
        src_ids: &[Vec<u8>],
        filter_pushdown_buf: &[u8],
    ) -> VertexIter {
        let mut vertex_vec = vec![];

        let pushdown = decode_pushdown(filter_pushdown_buf);
        let filter_pushdown_map =
            convert_to_filter_pushdown_map(src_ids, &pushdown, &self.engine.label_dict);

        for src_id in src_ids {
            if filter_pushdown_map.contains_key(src_id) {
                vertex_vec.extend(self.get_vertex_with_filter_pushdown(
                    src_id,
                    filter_pushdown_map.get(src_id).unwrap(),
                ));
            } else {
                vertex_vec.extend(self.get_vertex(src_id));
            };
        }

        VertexIter::from_vec(vertex_vec)
    }

    /// Get serialized vertex iterator according to the src_ids and filter
    /// pushdown buffer.
    pub fn get_multi_serialized_vertex_with_filter_pushdown_buf(
        &self,
        src_ids: &[Vec<u8>],
        filter_pushdown_buf: &[u8],
    ) -> SerializedVertexIter {
        SerializedVertexIter::from_vertex_iter(
            self.get_multi_vertex_with_filter_pushdown_buf(src_ids, filter_pushdown_buf),
        )
    }

    /// Get edge iterator according to the src_ids and filter pushdown buffer.
    pub fn get_multi_edge_with_filter_pushdown_buf(
        &self,
        src_ids: &[Vec<u8>],
        filter_pushdown_buf: &[u8],
    ) -> EdgeIter {
        let mut edge_vec = vec![];

        let pushdown = decode_pushdown(filter_pushdown_buf);
        let filter_pushdown_map =
            convert_to_filter_pushdown_map(src_ids, &pushdown, &self.engine.label_dict);

        for src_id in src_ids {
            if filter_pushdown_map.contains_key(src_id) {
                edge_vec.extend(self.get_edge_with_filter_pushdown(
                    src_id,
                    filter_pushdown_map.get(src_id).unwrap(),
                ));
            } else {
                edge_vec.extend(self.get_edge(src_id));
            };
        }

        EdgeIter::from_vec(edge_vec)
    }

    /// Get serialized edge iterator according to the src_ids and filter
    /// pushdown buffer.
    pub fn get_multi_serialized_edge_with_filter_pushdown_buf(
        &self,
        src_ids: &[Vec<u8>],
        filter_pushdown_buf: &[u8],
    ) -> SerializedEdgeIter {
        SerializedEdgeIter::from_edge_iter(
            self.get_multi_edge_with_filter_pushdown_buf(src_ids, filter_pushdown_buf),
        )
    }

    /// Get vertex and edge iterator according to the src_ids and filter
    /// pushdown buffer.
    pub fn get_multi_vertex_and_edge_with_filter_pushdown_buf(
        &self,
        src_ids: &[Vec<u8>],
        filter_pushdown_buf: &[u8],
    ) -> VertexAndEdgeIter {
        let mut vertex_and_edge_iter_vec = vec![];

        let pushdown = decode_pushdown(filter_pushdown_buf);
        let filter_pushdown_map =
            convert_to_filter_pushdown_map(src_ids, &pushdown, &self.engine.label_dict);

        for src_id in src_ids {
            if filter_pushdown_map.contains_key(src_id) {
                let vertex_and_edge = self.get_vertex_and_edge_with_filter_pushdown(
                    src_id,
                    filter_pushdown_map.get(src_id).unwrap(),
                );
                if !vertex_and_edge.src_id.is_empty() {
                    vertex_and_edge_iter_vec.push(vertex_and_edge);
                }
            } else {
                let vertex_and_edge = self.get_vertex_and_edge(src_id);
                if !vertex_and_edge.src_id.is_empty() {
                    vertex_and_edge_iter_vec.push(vertex_and_edge);
                }
            };
        }

        VertexAndEdgeIter::from_vec(vertex_and_edge_iter_vec)
    }

    /// Get serialized vertex and edge iterator according to the src_ids and
    /// filter pushdown buffer.
    pub fn get_multi_serialized_vertex_and_edge_with_filter_pushdown_buf(
        &self,
        src_ids: &[Vec<u8>],
        filter_pushdown_buf: &[u8],
    ) -> SerializedVertexAndEdgeIter {
        SerializedVertexAndEdgeIter::from_vertex_and_edge_iter(
            self.get_multi_vertex_and_edge_with_filter_pushdown_buf(src_ids, filter_pushdown_buf),
        )
    }

    /// Scan the the whole graph engine and return vertex iterator.
    pub fn scan_vertex(&self) -> VertexIter {
        VertexIter::from_scan_iter(VertexScanIter::new(
            &self.graph_serde,
            &self.engine,
            create_empty_filter_pushdown(),
        ))
    }

    /// Scan the the whole graph engine and return serialized vertex iterator.
    pub fn scan_serialized_vertex(&mut self) -> SerializedVertexIter {
        SerializedVertexIter::from_vertex_iter(self.scan_vertex())
    }

    /// Scan the the whole graph engine and return vertex iterator according to
    /// the filter push-down.
    pub fn scan_vertex_with_filter(&self, pushdown_buf: &[u8]) -> VertexIter {
        let pushdown = decode_pushdown(pushdown_buf);
        let filter_pushdown = convert_to_filter_pushdown(&pushdown, &self.engine.label_dict);

        VertexIter::from_scan_iter(VertexScanIter::new(
            &self.graph_serde,
            &self.engine,
            filter_pushdown,
        ))
    }

    /// Scan the the whole graph engine and return serialized vertex iterator
    /// according to the filter push-down.
    pub fn scan_serialized_vertex_with_filter(
        &mut self,
        pushdown_buf: &[u8],
    ) -> SerializedVertexIter {
        SerializedVertexIter::from_vertex_iter(self.scan_vertex_with_filter(pushdown_buf))
    }

    /// Scan the the whole graph engine and return edge iterator according to
    /// the filter push-down.
    pub fn scan_edge(&self) -> EdgeIter {
        EdgeIter::from_scan_iter(EdgeScanIter::new(
            &self.graph_serde,
            &self.engine,
            create_empty_filter_pushdown(),
        ))
    }

    /// Scan the the whole graph engine and return serialized edge iterator
    /// according to the filter push-down.
    pub fn scan_serialized_edge(&mut self) -> SerializedEdgeIter {
        SerializedEdgeIter::from_edge_iter(self.scan_edge())
    }

    /// Scan the the whole graph engine and return edge iterator according to
    /// the filter push-down.
    pub fn scan_edge_with_filter(&self, pushdown_buf: &[u8]) -> EdgeIter {
        let pushdown = decode_pushdown(pushdown_buf);
        let filter_pushdown = convert_to_filter_pushdown(&pushdown, &self.engine.label_dict);

        EdgeIter::from_scan_iter(EdgeScanIter::new(
            &self.graph_serde,
            &self.engine,
            filter_pushdown,
        ))
    }

    /// Scan the the whole graph engine and return serialized edge iterator
    /// according to the filter push-down.
    pub fn scan_serialized_edge_with_filter(&mut self, pushdown_buf: &[u8]) -> SerializedEdgeIter {
        SerializedEdgeIter::from_edge_iter(self.scan_edge_with_filter(pushdown_buf))
    }

    /// Scan the the whole graph engine and return vertex and edge iterator.
    pub fn scan_vertex_and_edge(&self) -> VertexAndEdgeIter {
        VertexAndEdgeIter::from_scan_iter(VertexAndEdgeScanIter::new(
            &self.graph_serde,
            &self.engine,
            create_empty_filter_pushdown(),
        ))
    }

    /// Scan the the whole graph engine and return serialized vertex and edge
    /// iterator.
    pub fn scan_serialized_vertex_and_edge(&mut self) -> SerializedVertexAndEdgeIter {
        SerializedVertexAndEdgeIter::from_vertex_and_edge_iter(self.scan_vertex_and_edge())
    }

    /// Scan the the whole graph engine and return vertex and edge iterator
    /// according to the filter push-down.
    pub fn scan_vertex_and_edge_with_filter(&self, pushdown_buf: &[u8]) -> VertexAndEdgeIter {
        let pushdown = decode_pushdown(pushdown_buf);
        let filter_pushdown = convert_to_filter_pushdown(&pushdown, &self.engine.label_dict);

        VertexAndEdgeIter::from_scan_iter(VertexAndEdgeScanIter::new(
            &self.graph_serde,
            &self.engine,
            filter_pushdown,
        ))
    }

    /// Scan the the whole graph engine and return serialized vertex and edge
    /// iterator according to the filter push-down.
    pub fn scan_serialized_vertex_and_edge_with_filter(
        &mut self,
        pushdown_buf: &[u8],
    ) -> SerializedVertexAndEdgeIter {
        SerializedVertexAndEdgeIter::from_vertex_and_edge_iter(
            self.scan_vertex_and_edge_with_filter(pushdown_buf),
        )
    }

    /// Flush data from memory to disk.
    pub fn flush(&mut self) {
        self.engine.flush();
    }

    /// Close engine.
    /// This function don't guarantee data persistent, need flush before
    /// calling close.
    pub fn close(&mut self) {
        self.engine.flush();
        self.engine.close();
    }

    /// Archive graph engine and record the version to reliable storage medium
    /// for persistent.
    pub fn archive(&mut self, version: u64) {
        self.engine.flush();
        self.engine.archive(version)
    }

    /// Recover graph engine from reliable storage medium.
    pub fn recover(&mut self, version: u64) {
        self.engine.recover(version)
    }

    /// manually trigger data compaction.
    pub fn compact(&mut self) {
        self.engine.flush();
        self.engine.compact()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use bytes::{Buf, BufMut};
    use prost::Message;

    use crate::{
        api::{
            filter::converter::encode_pushdown,
            graph::{edge::Edge, vertex::Vertex, EdgeDirection},
        },
        gen::pushdown::{
            filter_node::{Content, Content::LongContent},
            push_down::Filter,
            EdgeLimit, FilterNode, FilterNodes, LongList, PushDown, StringList,
        },
        log_util::{self, LogLevel, LogType},
        test_util::{bind_job_name_with_ts, delete_test_dir},
        CStore, CStoreConfigBuilder, ConfigMap, SIZE_OF_U32,
    };

    #[test]
    fn test_add_and_get() {
        log_util::try_init(LogType::ConsoleAndFile, LogLevel::Debug, 0);

        let mut config_map = ConfigMap::default();
        let job_name = bind_job_name_with_ts("test_add_and_get");
        config_map.insert("store.job_name", job_name.as_str());

        let config = CStoreConfigBuilder::default()
            .set_with_map(&config_map)
            .build();

        let table_config = Arc::clone(&config.table);

        let persistent_config = config.persistent.clone();
        let mut cstore = CStore::new(config);

        for i in 0..10000 {
            let mut src_id_bytes: Vec<u8> = Vec::with_capacity(SIZE_OF_U32);
            src_id_bytes.put_u32(i);

            let vertex = Vertex::create_id_time_label_vertex(
                src_id_bytes.clone(),
                i as u64,
                String::from("test"),
                src_id_bytes.clone(),
            );

            cstore.add_vertex(vertex);
        }
        cstore.flush();
        cstore.flush();

        let mut src_id_vec = vec![];
        for i in 0..10000 {
            let mut src_id_bytes: Vec<u8> = Vec::with_capacity(SIZE_OF_U32);
            src_id_bytes.put_u32(i);
            let vertex_iter = cstore.get_vertex(src_id_bytes.as_slice());
            for vertex in vertex_iter {
                assert_eq!(vertex.src_id(), vertex.property());
            }

            src_id_vec.push(src_id_bytes)
        }

        let mut count = 0;
        let vertex_iter = cstore
            .get_multi_vertex_with_filter_pushdown_buf(src_id_vec.as_slice(), vec![].as_slice());
        for vertex in vertex_iter {
            assert_eq!(vertex.src_id(), vertex.property());
            count = count + 1;
        }
        assert_eq!(count, 10000);

        let mut count = 0;
        let vertex_iter = cstore.get_multi_vertex_with_filter_pushdown_buf(
            src_id_vec.as_slice(),
            &encode_pushdown(&generate_ts_filter_pushdown_map(
                true,
                src_id_vec.as_slice(),
                10,
                20,
            )),
        );
        for vertex in vertex_iter {
            assert_eq!(vertex.src_id(), vertex.property());
            count = count + 1;
        }
        assert_eq!(count, 10);

        let mut count = 0;
        let vertex_iter = cstore.get_multi_vertex_with_filter_pushdown_buf(
            src_id_vec.as_slice(),
            &encode_pushdown(&generate_ts_filter_pushdown_map_with_single_node(
                true, 10, 20,
            )),
        );
        for vertex in vertex_iter {
            assert_eq!(vertex.src_id(), vertex.property());
            count = count + 1;
        }
        assert_eq!(count, 10);

        let mut count = 0;
        let vertex_scan_iter = cstore.scan_vertex();
        for vertex in vertex_scan_iter {
            assert_eq!(vertex.src_id(), vertex.property());
            count = count + 1;
        }
        assert_eq!(count, 10000);

        let mut count = 0;
        let vertex_scan_iter = cstore
            .scan_vertex_with_filter(&encode_pushdown(&generate_ts_filter_pushdown(true, 20, 40)));
        for vertex in vertex_scan_iter {
            assert_eq!(vertex.src_id(), vertex.property());
            count = count + 1;
        }
        assert_eq!(count, 20);

        for i in 0..10000 {
            let mut src_id_bytes: Vec<u8> = Vec::with_capacity(SIZE_OF_U32);
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

            cstore.add_edge(edge);
        }
        cstore.flush();
        cstore.flush();

        let mut src_id_vec = vec![];
        for i in 0..10000 {
            let mut src_id_bytes: Vec<u8> = Vec::with_capacity(SIZE_OF_U32);
            src_id_bytes.put_u32(i);
            let edge_iter = cstore.get_edge(src_id_bytes.as_slice());
            for edge in edge_iter {
                assert_eq!(edge.src_id(), edge.property());
            }

            src_id_vec.push(src_id_bytes)
        }

        let mut count = 0;
        let edge_iter = cstore
            .get_multi_edge_with_filter_pushdown_buf(src_id_vec.as_slice(), vec![].as_slice());
        for edge in edge_iter {
            assert_eq!(edge.src_id(), edge.property());
            count = count + 1;
        }
        assert_eq!(count, 10000);

        let mut count = 0;
        let edge_iter = cstore.get_multi_edge_with_filter_pushdown_buf(
            src_id_vec.as_slice(),
            &encode_pushdown(&generate_ts_filter_pushdown_map(
                false,
                src_id_vec.as_slice(),
                10,
                20,
            )),
        );
        for edge in edge_iter {
            assert_eq!(edge.src_id(), edge.property());
            count = count + 1;
        }
        assert_eq!(count, 10);

        let mut count = 0;
        let edge_scan_iter = cstore.scan_edge();
        for edge in edge_scan_iter {
            assert_eq!(edge.src_id(), edge.property());
            count = count + 1;
        }
        assert_eq!(count, 10000);

        let mut count = 0;
        let edge_scan_iter = cstore.scan_edge_with_filter(&encode_pushdown(
            &generate_ts_filter_pushdown(false, 100, 200),
        ));
        for edge in edge_scan_iter {
            assert_eq!(edge.src_id(), edge.property());
            count = count + 1;
        }
        assert_eq!(count, 100);

        let mut count = 0;
        let vertex_and_edge_iter = cstore.get_multi_vertex_and_edge_with_filter_pushdown_buf(
            src_id_vec.as_slice(),
            vec![].as_slice(),
        );
        for vertex_and_edge in vertex_and_edge_iter {
            for vertex in vertex_and_edge.vertex_iter {
                assert_eq!(vertex_and_edge.src_id.as_slice(), vertex.src_id());
                assert_eq!(vertex.src_id(), vertex.property());
                count = count + 1;
            }
            for edge in vertex_and_edge.edge_iter {
                assert_eq!(vertex_and_edge.src_id.as_slice(), edge.src_id());
                assert_eq!(edge.src_id(), edge.property());
                count = count + 1;
            }
        }
        assert_eq!(count, 20000);

        let vertex_and_edge_iter = cstore.get_multi_vertex_and_edge_with_filter_pushdown_buf(
            src_id_vec.as_slice(),
            &encode_pushdown(&generate_ts_filter_pushdown_map(
                false,
                src_id_vec.as_slice(),
                40,
                50,
            )),
        );
        let mut vertex_count = 0;
        let mut edge_count = 0;
        for vertex_and_edge in vertex_and_edge_iter {
            for vertex in vertex_and_edge.vertex_iter {
                assert_eq!(vertex_and_edge.src_id.as_slice(), vertex.src_id());
                assert_eq!(vertex.src_id(), vertex.property());
                vertex_count = vertex_count + 1;
            }
            for edge in vertex_and_edge.edge_iter {
                assert_eq!(vertex_and_edge.src_id.as_slice(), edge.src_id());
                assert_eq!(edge.src_id(), edge.property());
                edge_count = edge_count + 1;
            }
        }
        assert_eq!(vertex_count, 10000);
        assert_eq!(edge_count, 10);

        let mut count = 0;
        let vertex_and_edge_scan_iter = cstore.scan_vertex_and_edge();
        for vertex_and_edge in vertex_and_edge_scan_iter {
            for vertex in vertex_and_edge.vertex_iter {
                assert_eq!(vertex_and_edge.src_id.as_slice(), vertex.src_id());
                assert_eq!(vertex.src_id(), vertex.property());
                count = count + 1;
            }
            for edge in vertex_and_edge.edge_iter {
                assert_eq!(vertex_and_edge.src_id.as_slice(), edge.src_id());
                assert_eq!(edge.src_id(), edge.property());
                count = count + 1;
            }
        }
        assert_eq!(count, 20000);

        let mut vertex_count = 0;
        let mut edge_count = 0;
        let vertex_and_edge_scan_iter = cstore.scan_vertex_and_edge_with_filter(&encode_pushdown(
            &generate_ts_filter_pushdown(true, 20, 30),
        ));
        for vertex_and_edge in vertex_and_edge_scan_iter {
            for vertex in vertex_and_edge.vertex_iter {
                assert_eq!(vertex_and_edge.src_id.as_slice(), vertex.src_id());
                assert_eq!(vertex.src_id(), vertex.property());
                vertex_count = vertex_count + 1;
            }
            for edge in vertex_and_edge.edge_iter {
                assert_eq!(vertex_and_edge.src_id.as_slice(), edge.src_id());
                assert_eq!(edge.src_id(), edge.property());
                edge_count = edge_count + 1;
            }
        }
        assert_eq!(vertex_count, 10);
        assert_eq!(edge_count, 10000);

        cstore.close();

        delete_test_dir(&table_config, &persistent_config);
    }

    fn generate_ts_filter_pushdown_map_with_single_node(
        is_vertex: bool,
        start_ts: i64,
        end_ts: i64,
    ) -> PushDown {
        let filter_node = FilterNode {
            filter_type: if is_vertex { 4 } else { 5 },
            filters: vec![],
            content: Some(LongContent(LongList {
                long: vec![start_ts, end_ts],
            })),
        };
        PushDown {
            filter: Some(Filter::FilterNode(filter_node)),
            sort_type: vec![],
            edge_limit: None,
        }
    }

    fn generate_ts_filter_pushdown_map(
        is_vertex: bool,
        src_ids: &[Vec<u8>],
        start_ts: i64,
        end_ts: i64,
    ) -> PushDown {
        let keys = Vec::from(src_ids);
        let mut filter_nodes = vec![];
        for _i in 0..keys.len() {
            filter_nodes.push(FilterNode {
                filter_type: if is_vertex { 4 } else { 5 },
                filters: vec![],
                content: Some(LongContent(LongList {
                    long: vec![start_ts, end_ts],
                })),
            })
        }
        PushDown {
            filter: Some(Filter::FilterNodes(FilterNodes { keys, filter_nodes })),
            sort_type: vec![],
            edge_limit: None,
        }
    }

    fn generate_ts_filter_pushdown(is_vertex: bool, start_ts: i64, end_ts: i64) -> PushDown {
        let filter_node = FilterNode {
            filter_type: if is_vertex { 4 } else { 5 },
            filters: vec![],
            content: Some(LongContent(LongList {
                long: vec![start_ts, end_ts],
            })),
        };
        PushDown {
            filter: Some(Filter::FilterNode(filter_node)),
            sort_type: vec![],
            edge_limit: None,
        }
    }

    #[test]
    fn test_pushdown() {
        log_util::try_init(LogType::ConsoleAndFile, LogLevel::Debug, 0);

        let mut config_map = ConfigMap::default();

        let job_name = bind_job_name_with_ts("test_pushdown");
        config_map.insert("store.job_name", job_name.as_str());

        let config = CStoreConfigBuilder::default()
            .set_with_map(&config_map)
            .build();

        let table_config = Arc::clone(&config.table);
        let persistent_config = config.persistent.clone();
        let mut cstore = CStore::new(config);

        let src_id_1 = String::from("id1");
        let label1 = String::from("label1");
        cstore.add_vertex(Vertex::create_id_time_label_vertex(
            src_id_1.clone().encode_to_vec(),
            1,
            label1.clone(),
            vec![],
        ));

        let target_id_1 = String::from("id2");
        let target_id_2 = String::from("id3");
        let label2 = String::from("label2");
        cstore.add_edge(Edge::create_id_time_label_edge(
            src_id_1.clone().encode_to_vec(),
            target_id_1.clone().encode_to_vec(),
            10,
            label1.clone(),
            EdgeDirection::Out,
            vec![],
        ));
        cstore.add_edge(Edge::create_id_time_label_edge(
            src_id_1.clone().encode_to_vec(),
            target_id_2.clone().encode_to_vec(),
            10,
            label2.clone(),
            EdgeDirection::In,
            vec![],
        ));

        cstore.add_edge(Edge::create_id_time_label_edge(
            src_id_1.clone().encode_to_vec(),
            target_id_2.clone().encode_to_vec(),
            10,
            label2.clone(),
            EdgeDirection::Out,
            vec![],
        ));

        cstore.add_edge(Edge::create_id_time_label_edge(
            src_id_1.clone().encode_to_vec(),
            target_id_2.clone().encode_to_vec(),
            100,
            label2.clone(),
            EdgeDirection::Out,
            vec![],
        ));

        // test only vertex filter.
        let pushdown = PushDown {
            filter: Some(Filter::FilterNode(FilterNode {
                filter_type: 1,
                filters: vec![],
                content: None,
            })),
            edge_limit: None,
            sort_type: vec![],
        };

        let vertex_and_edge = cstore.get_vertex_and_edge_with_filter_pushdown_buf(
            src_id_1.clone().encode_to_vec().as_slice(),
            encode_pushdown(&pushdown).as_slice(),
        );
        let mut vertex_iter = vertex_and_edge.vertex_iter;
        let vertex = vertex_iter.next().unwrap();
        assert_eq!(vertex.src_id(), src_id_1.clone().encode_to_vec().as_slice());
        assert_eq!(vertex.label(), label1);
        let mut edge_iter = vertex_and_edge.edge_iter;
        assert!(edge_iter.next().is_none());

        // test out edge filter
        let pushdown = PushDown {
            filter: Some(Filter::FilterNode(FilterNode {
                filter_type: 3,
                filters: vec![],
                content: None,
            })),
            edge_limit: None,
            sort_type: vec![],
        };
        let vertex_and_edge = cstore.get_vertex_and_edge_with_filter_pushdown_buf(
            src_id_1.clone().encode_to_vec().as_slice(),
            encode_pushdown(&pushdown).as_slice(),
        );
        let mut vertex_iter = vertex_and_edge.vertex_iter;
        assert!(vertex_iter.next().is_some());

        let mut edge_iter = vertex_and_edge.edge_iter;
        edge_iter.next().unwrap();
        edge_iter.next().unwrap();
        edge_iter.next().unwrap();
        let edge = edge_iter.next();
        assert!(edge.is_none());

        // test in edge filter
        let pushdown = PushDown {
            filter: Some(Filter::FilterNode(FilterNode {
                filter_type: 2,
                filters: vec![],
                content: None,
            })),
            edge_limit: None,
            sort_type: vec![],
        };
        let vertex_and_edge = cstore.get_vertex_and_edge_with_filter_pushdown_buf(
            src_id_1.clone().encode_to_vec().as_slice(),
            encode_pushdown(&pushdown).as_slice(),
        );
        let mut vertex_iter = vertex_and_edge.vertex_iter;
        assert!(vertex_iter.next().is_some());

        let mut edge_iter = vertex_and_edge.edge_iter;
        edge_iter.next().unwrap();
        let edge = edge_iter.next();
        assert!(edge.is_none());

        // test vertex label filter
        let pushdown = PushDown {
            filter: Some(Filter::FilterNode(FilterNode {
                filter_type: 7,
                filters: vec![],
                content: Some(Content::StrContent(StringList {
                    str: vec![label2.clone()],
                })),
            })),
            edge_limit: None,
            sort_type: vec![],
        };
        let vertex_and_edge = cstore.get_vertex_and_edge_with_filter_pushdown_buf(
            src_id_1.clone().encode_to_vec().as_slice(),
            encode_pushdown(&pushdown).as_slice(),
        );
        let mut vertex_iter = vertex_and_edge.vertex_iter;
        assert!(vertex_iter.next().is_none());

        let mut edge_iter = vertex_and_edge.edge_iter;
        edge_iter.next().unwrap();
        edge_iter.next().unwrap();
        edge_iter.next().unwrap();
        edge_iter.next().unwrap();
        let edge = edge_iter.next();
        assert!(edge.is_none());

        // test and filter.
        let only_vertex_filter_node = FilterNode {
            filter_type: 1,
            filters: vec![],
            content: None,
        };

        let vertex_label_filter_node = FilterNode {
            filter_type: 7,
            filters: vec![],
            content: Some(Content::StrContent(StringList {
                str: vec![label2.clone()],
            })),
        };

        let mut filter_nodes = vec![];
        filter_nodes.push(only_vertex_filter_node);
        filter_nodes.push(vertex_label_filter_node);
        let pushdown = PushDown {
            filter: Some(Filter::FilterNode(FilterNode {
                filter_type: 12,
                filters: filter_nodes,
                content: Some(Content::StrContent(StringList {
                    str: vec![label2.clone()],
                })),
            })),
            edge_limit: None,
            sort_type: vec![],
        };

        let vertex_and_edge = cstore.get_vertex_and_edge_with_filter_pushdown_buf(
            src_id_1.clone().encode_to_vec().as_slice(),
            encode_pushdown(&pushdown).as_slice(),
        );
        let mut vertex_iter = vertex_and_edge.vertex_iter;
        assert!(vertex_iter.next().is_none());

        // test or filter.
        let only_vertex_label_node = FilterNode {
            filter_type: 1,
            filters: vec![],
            content: None,
        };

        let vertex_label_filter_node = FilterNode {
            filter_type: 7,
            filters: vec![],
            content: Some(Content::StrContent(StringList {
                str: vec![label2.clone()],
            })),
        };

        let mut filter_nodes = vec![];
        filter_nodes.push(only_vertex_label_node);
        filter_nodes.push(vertex_label_filter_node);
        let pushdown = PushDown {
            filter: Some(Filter::FilterNode(FilterNode {
                filter_type: 13,
                filters: filter_nodes,
                content: Some(Content::StrContent(StringList {
                    str: vec![label2.clone()],
                })),
            })),
            edge_limit: None,
            sort_type: vec![],
        };

        let vertex_and_edge = cstore.get_vertex_and_edge_with_filter_pushdown_buf(
            src_id_1.clone().encode_to_vec().as_slice(),
            encode_pushdown(&pushdown).as_slice(),
        );
        let mut vertex_iter = vertex_and_edge.vertex_iter;
        assert!(vertex_iter.next().is_some());

        cstore.close();

        delete_test_dir(&table_config, &persistent_config);
    }

    #[test]
    fn test_edge_limit() {
        let mut config_map = ConfigMap::default();
        let job_name = bind_job_name_with_ts("test_edge_limit");
        config_map.insert("store.job_name", job_name.as_str());
        config_map.insert("store.max_buffer_size_in_segment", "1000000");

        let config = CStoreConfigBuilder::default()
            .set_with_map(&config_map)
            .build();

        let table_config = config.table.clone();
        let persistent_config = config.persistent.clone();
        let mut cstore = CStore::new(config);

        for i in 0..1000000 {
            let mut src_id_bytes: Vec<u8> = Vec::with_capacity(SIZE_OF_U32);
            src_id_bytes.put_u32(1);

            let mut target_id_bytes: Vec<u8> = Vec::with_capacity(SIZE_OF_U32);
            target_id_bytes.put_u32(i + 1);

            let edge = Edge::create_id_time_label_edge(
                src_id_bytes.clone(),
                target_id_bytes.clone(),
                i as u64,
                String::from("test"),
                if i % 2 == 0 {
                    EdgeDirection::In
                } else {
                    EdgeDirection::Out
                },
                src_id_bytes.clone(),
            );

            cstore.add_vertex(Vertex::create_id_vertex(
                src_id_bytes.clone(),
                src_id_bytes.clone(),
            ));
            cstore.add_edge(edge);
        }
        cstore.flush();

        let mut src_id_bytes: Vec<u8> = Vec::with_capacity(SIZE_OF_U32);
        src_id_bytes.put_u32(1);

        let pushdown = PushDown {
            filter: Some(Filter::FilterNode(FilterNode {
                filter_type: 0,
                filters: vec![],
                content: None,
            })),
            edge_limit: Some(EdgeLimit {
                out: 5,
                r#in: 4,
                is_single: false,
            }),
            sort_type: vec![],
        };

        let edge_iter =
            cstore.get_edge_with_filter_pushdown_buf(&src_id_bytes, &encode_pushdown(&pushdown));

        let mut count = 0;
        let mut expected_direction = EdgeDirection::Out;
        let mut expected_target_id = 1000000;
        for edge in edge_iter {
            count = count + 1;
            assert_eq!(*edge.direction(), expected_direction);
            assert_eq!(edge.target_id().get_u32(), expected_target_id);
            expected_target_id = expected_target_id - 2;

            if expected_target_id == 999990 {
                expected_target_id = 999999;
                expected_direction = EdgeDirection::In;
            }
        }

        assert_eq!(count, 9);

        let mut src_id_bytes: Vec<u8> = Vec::with_capacity(SIZE_OF_U32);
        src_id_bytes.put_u32(1);

        let filter_node1 = FilterNode {
            filter_type: 5,
            filters: vec![],
            content: Some(LongContent(LongList {
                long: vec![10, 100],
            })),
        };
        let filter_node2 = FilterNode {
            filter_type: 5,
            filters: vec![],
            content: Some(LongContent(LongList {
                long: vec![200000, 400000],
            })),
        };
        let pushdown = PushDown {
            filter: Some(Filter::FilterNode(FilterNode {
                filter_type: 13,
                filters: vec![filter_node1, filter_node2],
                content: None,
            })),
            edge_limit: Some(EdgeLimit {
                out: 5,
                r#in: 4,
                is_single: true,
            }),
            sort_type: vec![],
        };

        let expected_edges = vec![
            (399999, EdgeDirection::Out),
            (399997, EdgeDirection::Out),
            (399995, EdgeDirection::Out),
            (399993, EdgeDirection::Out),
            (399991, EdgeDirection::Out),
            (99, EdgeDirection::Out),
            (97, EdgeDirection::Out),
            (95, EdgeDirection::Out),
            (93, EdgeDirection::Out),
            (91, EdgeDirection::Out),
            (399998, EdgeDirection::In),
            (399996, EdgeDirection::In),
            (399994, EdgeDirection::In),
            (399992, EdgeDirection::In),
            (98, EdgeDirection::In),
            (96, EdgeDirection::In),
            (94, EdgeDirection::In),
            (92, EdgeDirection::In),
        ];
        let edge_iter =
            cstore.get_edge_with_filter_pushdown_buf(&src_id_bytes, &encode_pushdown(&pushdown));

        for edge in edge_iter.enumerate() {
            let expected_edge = expected_edges.get(edge.0).unwrap();

            assert_eq!(*edge.1.direction(), (*expected_edge).1);
            assert_eq!(edge.1.ts(), (*expected_edge).0);
        }

        cstore.close();

        delete_test_dir(&table_config, &persistent_config);
    }

    #[test]
    pub fn test_scan_limit() {
        let mut config_map = ConfigMap::default();
        let job_name = bind_job_name_with_ts("test_scan_limit");
        config_map.insert("store.job_name", job_name.as_str());
        config_map.insert("store.max_buffer_size_in_segment", "1000000");

        let config = CStoreConfigBuilder::default()
            .set_with_map(&config_map)
            .build();

        let table_config = config.table.clone();
        let persistent_config = config.persistent.clone();
        let mut cstore = CStore::new(config);

        for i in 0..100 {
            let mut src_id_bytes: Vec<u8> = Vec::with_capacity(SIZE_OF_U32);
            src_id_bytes.put_u32(i);

            for j in 0..100 {
                let mut target_id_bytes: Vec<u8> = Vec::with_capacity(SIZE_OF_U32);
                target_id_bytes.put_u32(j);

                let edge = Edge::create_id_time_label_edge(
                    src_id_bytes.clone(),
                    target_id_bytes.clone(),
                    j as u64,
                    String::from("test"),
                    if j % 2 == 0 {
                        EdgeDirection::In
                    } else {
                        EdgeDirection::Out
                    },
                    src_id_bytes.clone(),
                );

                cstore.add_vertex(Vertex::create_id_vertex(
                    src_id_bytes.clone(),
                    src_id_bytes.clone(),
                ));
                cstore.add_edge(edge);
            }
        }
        cstore.flush();

        let pushdown = PushDown {
            filter: None,
            edge_limit: Some(EdgeLimit {
                out: 1,
                r#in: 1,
                is_single: false,
            }),
            sort_type: vec![],
        };

        let mut count = 0;
        let vertex_and_edge_iter =
            cstore.scan_vertex_and_edge_with_filter(&encode_pushdown(&pushdown));
        for mut vertex_and_edge in vertex_and_edge_iter {
            assert!(vertex_and_edge.vertex_iter.next().is_some());
            for _edge in vertex_and_edge.edge_iter {
                count += 1;
            }
        }
        assert_eq!(count, 200);

        let edge_iter = cstore.scan_edge_with_filter(&encode_pushdown(&pushdown));
        let mut count = 0;
        for _edge in edge_iter {
            count += 1;
        }
        assert_eq!(count, 200);

        cstore.close();

        delete_test_dir(&table_config, &persistent_config);
    }

    #[test]
    pub fn test_load() {
        let mut config_map = ConfigMap::default();
        let job_name = bind_job_name_with_ts("test_load");
        config_map.insert("store.job_name", job_name.as_str());

        let config = CStoreConfigBuilder::default()
            .set_with_map(&config_map)
            .build();

        let table_config = config.table.clone();
        let persistent_config = config.persistent.clone();
        let mut cstore = CStore::new(config);

        for _i in 0..100 {
            let mut src_id_bytes: Vec<u8> = Vec::with_capacity(SIZE_OF_U32);
            src_id_bytes.put_u32(1);

            cstore.add_vertex(Vertex::create_id_vertex(
                src_id_bytes.clone(),
                src_id_bytes.clone(),
            ));
        }
        cstore.flush();

        let vertex_iterator = cstore.scan_vertex();
        let mut count = 0;
        for _vertex in vertex_iterator {
            count += 1;
        }
        assert_eq!(count, 1);

        cstore.close();
        delete_test_dir(&table_config, &persistent_config);
    }
}
