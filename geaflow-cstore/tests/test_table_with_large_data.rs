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

use std::sync::Arc;

use geaflow_cstore::{
    engine::dict::label_dict::LabelDict,
    log_util::{self, LogLevel, LogType},
    test_util::{
        bind_job_name_with_ts, delete_test_dir, test_table_append, test_table_read_randomly,
        test_table_read_sequentially, GraphDataGenerator, GraphDataGeneratorParameters,
        TableTestParameters, INTEGRATION_TEST_LOCAL_ROOT, INTEGRATION_TEST_REMOTE_ROOT,
    },
    CStoreConfig, CStoreConfigBuilder, ConfigMap, EngineContext,
};

use crate::log_util::info;

#[derive(PartialEq)]
enum TestMode {
    Local = 1,
}

fn build_test_parameters(
    test_mode: &TestMode,
) -> (
    CStoreConfig,
    GraphDataGeneratorParameters,
    TableTestParameters,
) {
    let mut config_map = ConfigMap::default();
    let job_name = bind_job_name_with_ts("integration_test_tables");
    config_map.insert("store.job_name", job_name.as_str());
    config_map.insert("local.root", INTEGRATION_TEST_LOCAL_ROOT);
    config_map.insert("persistent.root", INTEGRATION_TEST_REMOTE_ROOT);

    let cstore_config: CStoreConfig = CStoreConfigBuilder::default()
        .set_with_map(&config_map)
        .build();

    let property_len_interval_vec: Vec<(usize, usize)> = match test_mode {
        TestMode::Local => vec![
            (0, 1),
            (1, 50),
            (50, 100),
            (100, 1000),
            (1000, 5000),
            (5000, 10000),
            (10000, 25000),
            (25000, 50000),
        ],
    };

    let graph_data_gen_parameters = GraphDataGeneratorParameters::default()
        .with_random_switch(true)
        .with_generate_graph_data_num(10000)
        .with_target_id_len_interval_vec(vec![(4, 5)])
        .with_property_len_interval_vec(property_len_interval_vec);

    let (flush_table_size, max_loop_exec_times) = match test_mode {
        TestMode::Local => (268435456, 500000),
    };

    let table_test_parameters = TableTestParameters::default()
        .with_flush_table_size(flush_table_size)
        .with_max_loop_exec_times(max_loop_exec_times);

    (
        cstore_config,
        graph_data_gen_parameters,
        table_test_parameters,
    )
}

// Test the functional accuracy of the table, each round of reports is generated
// by the property of random length within a certain interval.
fn test_table(test_mode: &TestMode) {
    log_util::try_init(LogType::ConsoleAndFile, LogLevel::Debug, 0);

    let (cstore_config, graph_data_gen_parameters, table_test_parameters) =
        build_test_parameters(test_mode);
    let table_config = &cstore_config.table;
    let label_dict = Arc::new(LabelDict::default());

    // New a generator and Generate graphdatas of specific amount.
    let mut graph_generator = GraphDataGenerator::new(&graph_data_gen_parameters);
    let mut graph_data_vec_triad_vec = graph_generator.generate();

    // Test Table Append Single data & Read
    for (index, graph_data_vec_triad) in graph_data_vec_triad_vec.iter().enumerate() {
        let graph_data_vec = &graph_data_vec_triad.2;

        let property_len_interval = graph_data_gen_parameters
            .property_len_interval_vec
            .get(index)
            .unwrap();

        info!(
            "[test table append and read] start new round test, with random property len in interval [{}, {})",
            property_len_interval.0, property_len_interval.1
        );

        {
            let engine_context = Arc::new(EngineContext::new(
                &Arc::new(LabelDict::default()),
                &cstore_config,
            ));

            // Test table append, return a Vec<triple>, which describes [fid, block offset,
            // reference of block data] for each item.
            let vec_fid_offset = test_table_append(
                &cstore_config.table,
                graph_data_vec,
                1,
                table_test_parameters.max_loop_exec_times,
                table_test_parameters.flush_table_size,
                &engine_context.file_operator,
            );

            // Test table read sequentially.
            test_table_read_sequentially(
                &cstore_config.table,
                &engine_context.cache_handler,
                &vec_fid_offset,
                &engine_context.file_operator,
            );

            // Test table read randomly.
            test_table_read_randomly(
                &cstore_config.table,
                &engine_context.cache_handler,
                &vec_fid_offset,
                table_test_parameters.multi_read_thread_num,
                &engine_context.file_operator,
            );

            // Remove the dir read and written, to make sure tests under different property
            // size be not affected by the file length.
            delete_test_dir(&table_config, &cstore_config.persistent);
        }
    }

    // Test Table Append list with less data.
    graph_data_vec_triad_vec.pop();

    // Test Table Append list data & Read
    for (index, graph_data_vec_triad) in graph_data_vec_triad_vec.iter().enumerate() {
        let graph_data_vec = &graph_data_vec_triad.2;

        let property_len_interval = graph_data_gen_parameters
            .property_len_interval_vec
            .get(index)
            .unwrap();
        info!(
            "[test table append list and read] start new round test, with random property len in interval [{}, {})",
            property_len_interval.0, property_len_interval.1
        );

        {
            let engine_context = Arc::new(EngineContext::new(&label_dict, &cstore_config));

            // Test table append list, return a Vec<triple>, which describes [fid, block
            // offset, reference of block data] for each item.
            let vec_fid_offset = test_table_append(
                &cstore_config.table,
                graph_data_vec,
                table_test_parameters.property_num_default,
                table_test_parameters.max_loop_exec_times,
                table_test_parameters.flush_table_size,
                &engine_context.file_operator,
            );

            // Test table read sequentially.
            test_table_read_sequentially(
                &cstore_config.table,
                &engine_context.cache_handler,
                &vec_fid_offset,
                &engine_context.file_operator,
            );

            // Test table read randomly.
            test_table_read_randomly(
                &cstore_config.table,
                &engine_context.cache_handler,
                &vec_fid_offset,
                table_test_parameters.multi_read_thread_num,
                &engine_context.file_operator,
            );

            // Remove the dir read and written, to make sure tests under different property
            // size be not affected by the file length.
            delete_test_dir(&table_config, &cstore_config.persistent);
        }
    }
}

#[test]
#[ignore]
fn test_table_local_mode() {
    test_table(&TestMode::Local);
}
