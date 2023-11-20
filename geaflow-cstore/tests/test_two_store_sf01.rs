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

use std::collections::HashSet;

use geaflow_cstore::{
    log_util::{self, LogLevel, LogType},
    test_util::{
        delete_fo_test_dir, delete_test_dir, edge_iter_is_equal, vertex_iter_is_equal,
        TestStoreHelper,
    },
    CStore, LdbcSource,
};
use itertools::Itertools;
use prost::Message;
use rand::seq::SliceRandom;
use rustc_hash::FxHashMap;

use crate::log_util::info;

#[test]
fn test_two_store_with_ldbc_sf01_source() {
    log_util::try_init(LogType::ConsoleAndFile, LogLevel::Debug, 0);
    info!("start test_two_store_with_ldbc_sf01_source");

    let test1_name = String::from("test_ldbc_sf01_source_1");
    let test2_name = String::from("test_ldbc_sf01_source_2");

    let cstore_1_config = TestStoreHelper::randomly_construct_cstore_config(test1_name.as_str(), 0);
    let cstore_2_config = TestStoreHelper::randomly_construct_cstore_config(test2_name.as_str(), 1);
    let mut cstore1 = CStore::new(cstore_1_config.clone());
    let mut cstore2 = CStore::new(cstore_2_config.clone());

    // Load the ldbc source
    let mut ldbc_config = FxHashMap::default();
    // total record is 367188.
    ldbc_config.insert(String::from("local.ldbc.path"), String::from("/tmp/sf0.1"));
    let ldbc_source = LdbcSource::new(&ldbc_config);
    let data_iter = ldbc_source.get_iter();

    let mut id_set = HashSet::default();
    let mut count = 0;
    for data in data_iter {
        TestStoreHelper::insert_data(&data, &mut cstore1, None, None, Some(&mut id_set));
        TestStoreHelper::insert_data(&data, &mut cstore2, None, None, Some(&mut id_set));
        count += 1;
        if count % 40000 == 0 {
            cstore1.archive(count);
            cstore2.archive(count);

            cstore1.close();
            cstore2.close();

            cstore1 = CStore::new(cstore_1_config.clone());
            cstore2 = CStore::new(cstore_2_config.clone());

            cstore1.recover(count);
            cstore2.recover(count);
        }
    }

    info!("start to compare cstore1 and cstore2");
    let mut id_list = id_set.into_iter().collect_vec();
    id_list.shuffle(&mut rand::thread_rng());

    for id in id_list {
        let id_bytes = id.encode_to_vec();
        let mut vertex_iter1 = cstore1.get_vertex(id_bytes.as_slice());
        let mut vertex_iter2 = cstore2.get_vertex(id_bytes.as_slice());
        vertex_iter_is_equal(&mut vertex_iter1, &mut vertex_iter2);

        let mut edge_iter1 = cstore1.get_edge(id_bytes.as_slice());
        let mut edge_iter2 = cstore2.get_edge(id_bytes.as_slice());

        edge_iter_is_equal(&mut edge_iter1, &mut edge_iter2);
    }
    info!("finish comparing cstore1 and cstore2");

    cstore1.close();
    cstore2.close();

    delete_test_dir(&cstore_1_config.table, &cstore_1_config.persistent);
    delete_test_dir(&cstore_2_config.table, &cstore_2_config.persistent);
    delete_fo_test_dir(&cstore_1_config.manifest, &cstore_1_config.persistent);
    delete_fo_test_dir(&cstore_2_config.manifest, &cstore_2_config.persistent);

    info!("finish test_two_store_with_ldbc_sf01_source");
}
