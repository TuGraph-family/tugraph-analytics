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

use std::collections::HashMap;

use geaflow_cstore::{
    api::graph::graph_comparator::{cmp_edge, parse_graph_sort_field},
    log_util::{self, LogLevel, LogType},
    test_util::{delete_test_dir, edge_is_equal, vertex_is_equal, TestStoreHelper},
    CStore, LdbcSource,
};
use itertools::Itertools;
use prost::Message;
use rustc_hash::FxHashMap;

use crate::log_util::info;

#[test]
fn test_single_store_with_ldbc_sf01_source() {
    log_util::try_init(LogType::ConsoleAndFile, LogLevel::Debug, 0);
    info!("start test_single_store_with_ldbc_sf01_source");

    let test_name = String::from("test_ldbc_sf01_source_1");

    let cstore_config = TestStoreHelper::randomly_construct_cstore_config(test_name.as_str(), 0);
    let mut cstore = CStore::new(cstore_config.clone());

    let mut edges_map = HashMap::default();
    let mut vertex_map = HashMap::default();

    // Load the ldbc source
    let mut ldbc_config = FxHashMap::default();
    ldbc_config.insert(String::from("local.ldbc.path"), String::from("/tmp/sf0.1"));
    let ldbc_source = LdbcSource::new(&ldbc_config);
    let data_iter = ldbc_source.get_iter();

    for data in data_iter {
        TestStoreHelper::insert_data(
            &data,
            &mut cstore,
            Some(&mut edges_map),
            Some(&mut vertex_map),
            None,
        );
    }

    // init graph sort field.
    let graph_sort_field = parse_graph_sort_field("direction,disc_time,label,dst_id");

    for id_and_expected_vertex in vertex_map {
        let mut vertex_iter =
            cstore.get_vertex(id_and_expected_vertex.0.encode_to_vec().as_slice());
        let vertex = vertex_iter.next().unwrap();
        assert!(vertex_is_equal(vertex, id_and_expected_vertex.1));
    }

    for id_and_expected_edge_set in edges_map {
        let mut expected_edges = id_and_expected_edge_set.1.iter().collect_vec();
        expected_edges.sort_by(|edge1, edge2| cmp_edge(edge1, edge2, graph_sort_field.as_slice()));

        let mut actual_edges = cstore
            .get_edge(id_and_expected_edge_set.0.encode_to_vec().as_slice())
            .collect_vec();
        actual_edges.sort_by(|edge1, edge2| cmp_edge(edge1, edge2, graph_sort_field.as_slice()));

        assert_eq!(expected_edges.len(), actual_edges.len());

        for i in 0..actual_edges.len() {
            assert!(edge_is_equal(&expected_edges[i], &actual_edges[i]));
        }
    }

    cstore.close();

    delete_test_dir(&cstore_config.table, &cstore_config.persistent);

    info!("finish test_single_store_with_ldbc_sf01_source");
}
