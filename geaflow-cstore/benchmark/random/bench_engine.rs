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

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use geaflow_cstore::{
    test_util::{
        bind_job_name_with_ts, delete_test_dir, BenchParameters, GraphDataGenerator,
        GraphDataGeneratorParameters,
    },
    CStoreConfig, CStoreConfigBuilder, ConfigMap, Engine,
};

fn build_bench_parameters() -> (CStoreConfig, GraphDataGeneratorParameters, BenchParameters) {
    let mut config_map = ConfigMap::default();
    let job_name = bind_job_name_with_ts("build_engine_with_random_source");
    config_map.insert("store.job_name", job_name.as_str());
    config_map.insert("log.enable_log", "false");

    let cstore_config = CStoreConfigBuilder::default()
        .set_with_map(&config_map)
        .build();

    let graph_data_gen_parameters =
        GraphDataGeneratorParameters::default().with_generate_graph_data_num(1000000);

    let bench_parameters = BenchParameters::default();

    (cstore_config, graph_data_gen_parameters, bench_parameters)
}

fn criterion_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("bench_store_with_random_source");

    let (cstore_config, graph_data_gen_parameters, bench_parameters) = build_bench_parameters();

    // New a generator and Generate graphdata of specific amount.
    let mut graph_generator = GraphDataGenerator::new(&graph_data_gen_parameters);
    let graph_data_vec_triad_vec = graph_generator.generate();

    for graph_data_vec_triad in graph_data_vec_triad_vec.iter() {
        // The length of the graphdata's property in cur benchmark is 4, 16, 64...
        let property_len = graph_data_vec_triad.1 as u64;

        let cstore_config = cstore_config.clone();
        let table_config = Arc::clone(&cstore_config.table);
        let persistent_config = cstore_config.persistent.clone();

        let mut engine = Engine::new(cstore_config);

        // Set the iteration time of the benchmark by the length of the property, to
        // ensure that the number of iterations under each property exceeds one
        // million times, and effective samples of sample size specified are taken.
        group
            .throughput(Throughput::Bytes(property_len))
            .measurement_time(Duration::from_secs(
                bench_parameters.write_bench_measure_time,
            ))
            .sample_size(bench_parameters.sample_size);

        // Fetch an infinite number of elements via a cycle iterator such that the
        // benchmark terminates when the test time is reached
        let mut iter = graph_data_vec_triad.2.iter().cycle();

        // The test code in cur benchmark.
        group.bench_with_input(BenchmarkId::from_parameter(property_len), &(), |b, ()| {
            b.iter(|| {
                engine.put(iter.next().unwrap().0, iter.next().unwrap().1.clone());
            });
        });

        engine.flush();
        engine.close();

        // Remove the dir read and written, to make sure tests under different property
        // size be not affected by the file length.
        delete_test_dir(&table_config, &persistent_config);
    }

    group.finish();
}

// Entrance of cur benchmark, all the bench functions will be run with command
// `make bench mod=<bench_mod_name>` executed in terminal.
criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
