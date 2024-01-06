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

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use dashmap::DashMap;
use geaflow_cstore::{
    log_util,
    log_util::{LogLevel, LogType},
    test_util::{
        bind_job_name_with_ts, BenchParameters, MemIndexGenerator, MemIndexGeneratorParameters,
    },
    CStoreConfig, CStoreConfigBuilder, ConfigMap, CsrMemIndex, ParentNode,
};

fn build_bench_parameters() -> (CStoreConfig, MemIndexGeneratorParameters, BenchParameters) {
    let mut config_map = ConfigMap::default();

    let job_name = bind_job_name_with_ts("build_mem_index");
    config_map.insert("store.job_name", job_name.as_str());

    let cstore_config = CStoreConfigBuilder::default()
        .set_with_map(&config_map)
        .build();

    let mem_index_gen_parameters = MemIndexGeneratorParameters::default().with_random_switch(true);

    let bench_parameters = BenchParameters::default()
        .with_default_bench_measure_time(10)
        .with_sample_size(10);

    (cstore_config, mem_index_gen_parameters, bench_parameters)
}

fn criterion_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("bench_mem_index");

    let (cstore_config, mem_index_gen_parameters, bench_parameters) = build_bench_parameters();

    let index_config = &cstore_config.index;

    let csr_mem_index = Arc::new(CsrMemIndex::new(index_config));
    let dash_map: Arc<DashMap<u32, ParentNode>> = Arc::new(DashMap::default());

    let mem_index_generator = MemIndexGenerator::new(&mem_index_gen_parameters);
    let mem_index_value_vec = Arc::new(mem_index_generator.mem_index_value);

    let threads_num = mem_index_gen_parameters.thread_num;
    let single_thread_throughput = mem_index_gen_parameters.key_interval.1 / threads_num;

    group
        .measurement_time(Duration::from_secs(
            bench_parameters.default_bench_measure_time,
        ))
        .sample_size(bench_parameters.sample_size);

    group.bench_with_input(
        BenchmarkId::from_parameter("DashMapMemIndex"),
        &(),
        |b, ()| {
            b.iter(|| {
                // Multi-thread job handle vec.
                let mut handles = Vec::new();

                for i in 0..threads_num {
                    let mem_index_value_vec = Arc::clone(&mem_index_value_vec);
                    let dash_map = Arc::clone(&dash_map);

                    let handle = std::thread::spawn(move || {
                        for j in 0..single_thread_throughput {
                            let val = mem_index_value_vec
                                .get((i * single_thread_throughput + j) as usize)
                                .unwrap();

                            dash_map.entry(val.0).or_insert(ParentNode::new());
                            dash_map.entry(val.0).and_modify(|parent_node| {
                                parent_node.append(val.1.clone());
                            });
                        }

                        for j in 0..single_thread_throughput {
                            let val = mem_index_value_vec
                                .get((i * single_thread_throughput + j) as usize)
                                .unwrap();

                            let _value = dash_map.get(&val.0).unwrap();
                        }
                    });
                    handles.push(handle);
                }

                for handle in handles {
                    handle.join().unwrap();
                }

                dash_map.clear();
            });
        },
    );

    group.bench_with_input(BenchmarkId::from_parameter("CsrMemIndex"), &(), |b, ()| {
        b.iter(|| {
            // Multi-thread job handle vec.
            let mut handles = Vec::new();

            for i in 0..threads_num {
                let csr_mem_index = Arc::clone(&csr_mem_index);
                let mem_index_value_vec = Arc::clone(&mem_index_value_vec);

                let handle = std::thread::spawn(move || {
                    for j in 0..single_thread_throughput {
                        let val = mem_index_value_vec
                            .get((i * single_thread_throughput + j) as usize)
                            .unwrap();
                        csr_mem_index.apply(val.0, |parent_node| {
                            parent_node.append(val.1.clone());
                        });
                    }

                    for j in 0..single_thread_throughput {
                        let val = mem_index_value_vec
                            .get((i * single_thread_throughput + j) as usize)
                            .unwrap();

                        let _value = csr_mem_index.get(&val.0).unwrap();
                    }
                });
                handles.push(handle);
            }

            for handle in handles {
                handle.join().unwrap();
            }

            csr_mem_index.close();
        });
    });

    group.finish();
}

// Entrance of cur benchmark, all the bench functions will be run with command
// `make bench mod=<bench_mod_name>` executed in terminal.
criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
