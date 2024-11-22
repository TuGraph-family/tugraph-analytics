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

use std::{sync::Arc, time::Duration};

use geaflow_cstore::{
    api::graph::graph_serde::GraphSerde,
    engine::dict::label_dict::LabelDict,
    log_util::{self, LogLevel, LogType},
    test_util::{
        bind_job_name_with_ts, delete_fo_test_dir, delete_test_dir, GraphDataBuilder,
        INTEGRATION_TEST_LOCAL_ROOT, INTEGRATION_TEST_REMOTE_ROOT,
    },
    CStoreConfigBuilder, ConfigMap, Engine,
};
use itertools::Itertools;
use rand::{seq::SliceRandom, Rng, SeedableRng};
use rand_isaac::IsaacRng;
use rustc_hash::FxHashSet;

use crate::log_util::debug;

const COMPACT_KEY_RANGE: [u32; 4] = [10000, 50000, 100000, 200000];
const COMPACT_ITERATIONS: [u32; 4] = [10000, 50000, 100000, 200000];
const COMPACT_TEST_ITERATIONS: u32 = 10;

#[test]
#[ignore]
fn test_engine_compact_without_fo() {
    log_util::try_init(LogType::ConsoleAndFile, LogLevel::Debug, 0);
    let mut rng: IsaacRng = SeedableRng::from_entropy();

    for _i in 0..COMPACT_TEST_ITERATIONS {
        let compact_iteration = *COMPACT_ITERATIONS.choose(&mut rng).unwrap();
        let compact_key_range = *COMPACT_KEY_RANGE.choose(&mut rng).unwrap();

        debug!(
            "compact_iteration {}, compact_key_range {}",
            compact_iteration, compact_key_range
        );

        let mut config_map = ConfigMap::default();

        let job_name = bind_job_name_with_ts("test_engine_compact");

        config_map.insert("local.root", INTEGRATION_TEST_LOCAL_ROOT);
        config_map.insert("persistent.root", INTEGRATION_TEST_REMOTE_ROOT);
        config_map.insert("store.job_name", job_name.as_str());
        config_map.insert("store.mem_segment_size", "5120");
        config_map.insert("store.level1_file_size", "16384");
        config_map.insert("store.file_size_multiplier", "4");
        config_map.insert("store.level0_file_num", "4");
        config_map.insert("index.index_granularity", "128");

        config_map.insert("store.compact_thread_num", "8");
        config_map.insert("store.max_buffer_size_in_segment", "1048");
        config_map.insert("store.compactor_interval", "1");
        config_map.insert("table.block_size", "512");
        config_map.insert("store.compactor_gc_ratio", "1.2");

        let cstore_config = CStoreConfigBuilder::default()
            .set_with_map(&config_map)
            .build();

        let table_config = Arc::clone(&cstore_config.table);
        let manifest_config = cstore_config.manifest.clone();
        let persistent_config = cstore_config.persistent.clone();

        let graph_serde = GraphSerde::default();
        let label_dict = Arc::new(LabelDict::default());

        debug!("config {:?}", &cstore_config.store);
        let mut engine = Engine::new(cstore_config);
        let sleep_interval = 60000000 / compact_iteration as u64;

        let test_key_vec = (0..compact_iteration)
            .map(|_| rng.gen_range(1..=compact_key_range))
            .collect_vec();
        let mut write_key_vec = vec![];

        for (index, i) in test_key_vec.iter().enumerate() {
            engine.put(
                *i,
                GraphDataBuilder::build_edge_graph_data(*i, &graph_serde, &label_dict),
            );
            engine.put(
                *i,
                GraphDataBuilder::build_vertex_graph_data(*i, &graph_serde, &label_dict),
            );
            write_key_vec.push(*i);

            if index != 0 && index as u32 % (compact_iteration / 100) == 0 {
                for _j in 1..100 {
                    let read_key = *write_key_vec.choose(&mut rand::thread_rng()).unwrap();

                    GraphDataBuilder::get_and_assert_data(&engine, read_key);
                }

                debug!(
                    "compact test, get and assert data passed, iterations {}",
                    index
                );
            }

            std::thread::sleep(Duration::from_micros(sleep_interval));
        }
        engine.flush();

        let test_read_set: FxHashSet<u32> = test_key_vec.into_iter().collect();
        let mut test_read_vec = test_read_set.into_iter().collect_vec();
        test_read_vec.sort();

        for i in test_read_vec {
            GraphDataBuilder::get_and_assert_data(&engine, i);
        }

        std::thread::sleep(Duration::from_secs(120));

        engine.close();

        delete_test_dir(&table_config, &persistent_config);
        delete_fo_test_dir(&manifest_config, &persistent_config);
    }
}
