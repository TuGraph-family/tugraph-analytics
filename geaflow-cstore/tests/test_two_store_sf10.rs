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

use geaflow_cstore::{
    log_util::{self, LogLevel, LogType},
    s_info,
    test_util::{
        delete_fo_test_dir, delete_test_dir, edge_iter_is_equal, vertex_iter_is_equal,
        TestStoreHelper,
    },
    CStore, LdbcSource,
};
use rustc_hash::FxHashMap;

use crate::log_util::info;

#[test]
#[ignore]
fn test_two_store_with_ldbc_sf10_source() {
    log_util::try_init(LogType::ConsoleAndFile, LogLevel::Debug, 0);
    info!("start test_two_store_with_ldbc_sf10_source");

    let test1_name = String::from("test_ldbc_sf10_source_1");
    let test2_name = String::from("test_ldbc_sf10_source_2");

    let cstore_1_config = TestStoreHelper::randomly_construct_cstore_config(test1_name.as_str(), 0);
    let cstore_2_config = TestStoreHelper::randomly_construct_cstore_config(test2_name.as_str(), 1);
    let mut cstore1 = CStore::new(cstore_1_config.clone());
    let mut cstore2 = CStore::new(cstore_2_config.clone());

    // Load the ldbc source
    let mut ldbc_config = FxHashMap::default();
    ldbc_config.insert(
        String::from("local.ldbc.path"),
        String::from("/tmp/composite-projected-fk"),
    );

    let ldbc_source1 = LdbcSource::new(&ldbc_config);
    let ldbc_source2 = LdbcSource::new(&ldbc_config);
    let mut data_iter1 = ldbc_source1.get_iter().peekable();
    let mut data_iter2 = ldbc_source2.get_iter().peekable();

    let mut insert_data_count: u128 = 0;

    s_info!(cstore_2_config.shard_index, "start to insert data");

    loop {
        if data_iter1.peek().is_none() {
            break;
        }

        TestStoreHelper::insert_data(&data_iter1.next().unwrap(), &mut cstore1, None, None, None);
        TestStoreHelper::insert_data(&data_iter2.next().unwrap(), &mut cstore2, None, None, None);

        insert_data_count += 1;
    }

    s_info!(
        cstore_2_config.shard_index,
        "finish insert data, count {}",
        insert_data_count
    );

    info!("start to compare cstore1 and cstore2");
    let record_cycle = insert_data_count / 1000;
    let mut get_data_count: u128 = 0;

    let mut vertex_and_edge_iter1 = cstore1.scan_vertex_and_edge();
    loop {
        let vertex_and_edge1_opt = vertex_and_edge_iter1.next();
        if vertex_and_edge1_opt.is_none() {
            break;
        }
        let mut vertex_and_edge1 = vertex_and_edge1_opt.unwrap();
        let mut vertex_and_edge2 = cstore2.get_vertex_and_edge(vertex_and_edge1.src_id.as_slice());

        let vertex_iter1 = &mut vertex_and_edge1.vertex_iter;
        let vertex_iter2 = &mut vertex_and_edge2.vertex_iter;
        vertex_iter_is_equal(vertex_iter1, vertex_iter2);

        let edge_iter1 = &mut vertex_and_edge1.edge_iter;
        let edge_iter2 = &mut vertex_and_edge2.edge_iter;
        edge_iter_is_equal(edge_iter1, edge_iter2);

        get_data_count += 1;
        if get_data_count % record_cycle == 0 {
            info!(
                "continue comparing cstore1 and cstore2, count {}",
                get_data_count
            );
        }
    }
    info!(
        "finish comparing cstore1 and cstore2, count {}",
        get_data_count
    );

    cstore1.close();
    cstore2.close();

    delete_test_dir(&cstore_1_config.table, &cstore_1_config.persistent);
    delete_test_dir(&cstore_2_config.table, &cstore_2_config.persistent);
    delete_fo_test_dir(&cstore_1_config.manifest, &cstore_1_config.persistent);
    delete_fo_test_dir(&cstore_2_config.manifest, &cstore_2_config.persistent);

    info!("finish test_two_store_with_ldbc_sf10_source");
}
