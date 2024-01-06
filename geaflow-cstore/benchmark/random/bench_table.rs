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

use std::{collections::HashSet, sync::Arc, time::Duration};

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use geaflow_cstore::{
    engine::dict::label_dict::LabelDict,
    test_util::{
        bind_job_name_with_ts, delete_test_dir, BenchParameters, GraphDataGenerator,
        GraphDataGeneratorParameters,
    },
    CStoreConfig, CStoreConfigBuilder, ConfigMap, CstoreFileOperator, EngineContext, GraphData,
    ReadableTable, SequentialReadIter, TableContext, WritableTable,
};
use rand::{seq::SliceRandom, SeedableRng};
use rand_isaac::IsaacRng;

pub const SEQUENTIAL_READ_SAMPLE_SIZE: usize = 100;

fn build_bench_parameters() -> (CStoreConfig, GraphDataGeneratorParameters, BenchParameters) {
    let mut config_map = ConfigMap::default();

    let job_name = bind_job_name_with_ts("bench_table");
    config_map.insert("store.job_name", job_name.as_str());

    let cstore_config = CStoreConfigBuilder::default()
        .set_with_map(&config_map)
        .build();

    let graph_data_gen_parameters =
        GraphDataGeneratorParameters::default().with_generate_graph_data_num(1000000);

    let bench_parameters = BenchParameters::default();

    (cstore_config, graph_data_gen_parameters, bench_parameters)
}

fn bench_write_table(
    cstore_config: &CStoreConfig,
    bench_parameters: &BenchParameters,
    property_len: &u64,
    graph_data_vec: &[(u32, GraphData)],
    group: &mut criterion::BenchmarkGroup<'_, criterion::measurement::WallTime>,
) -> Vec<Vec<(u32, u64)>> {
    let mem_segment_size = cstore_config.store.mem_segment_size as u64;

    group
        .throughput(Throughput::Bytes(*property_len))
        .measurement_time(Duration::from_secs(
            bench_parameters.write_bench_measure_time,
        ))
        .sample_size(bench_parameters.sample_size);

    // Fetch an infinite number of elements via a cycle iterator such that the
    // benchmark terminates when the test time is reached
    let mut iter = graph_data_vec.iter().cycle();
    let mut vec_fid_offset: Vec<Vec<(u32, u64)>> = Vec::new();
    vec_fid_offset.push(Vec::new());

    // The test code in cur benchmark.
    let parameter = String::from("write_") + u64::to_string(property_len).as_ref();
    let mut fid: u32 = 0;
    let mut item_num = 0;

    let file_operator = Arc::new(CstoreFileOperator::new(&cstore_config.persistent).unwrap());

    let table_context = TableContext::new(&cstore_config.table, fid, &file_operator);
    let mut writable_table = WritableTable::new(&cstore_config.table, table_context).unwrap();

    group.bench_with_input(BenchmarkId::from_parameter(parameter), &(), |b, ()| {
        b.iter(|| {
            let offset = writable_table
                .append(&iter.next().unwrap().1.property)
                .unwrap();

            if item_num < bench_parameters.bench_vec_max_size {
                vec_fid_offset
                    .get_mut(fid as usize)
                    .unwrap()
                    .push((fid, offset));
                item_num += 1;
            }

            if writable_table.get_table_offset() / mem_segment_size > 0 {
                if item_num < bench_parameters.bench_vec_max_size {
                    vec_fid_offset.push(Vec::new());
                }
                // finish cur iteration.x
                writable_table.finish().unwrap();
                writable_table.close();

                fid += 1;

                writable_table = WritableTable::new(
                    &&cstore_config.table,
                    TableContext::new(&&cstore_config.table, fid, &file_operator),
                )
                .unwrap();
            }
        });
    });

    writable_table.finish().unwrap();
    writable_table.close();

    vec_fid_offset
}

fn bench_read_table(
    cstore_config: &CStoreConfig,
    bench_parameters: &BenchParameters,
    vec_fid_offset: &Vec<Vec<(u32, u64)>>,
    property_len: &u64,
    group: &mut criterion::BenchmarkGroup<'_, criterion::measurement::WallTime>,
) {
    group
        .measurement_time(Duration::from_secs(
            bench_parameters.read_bench_measure_time,
        ))
        .sample_size(bench_parameters.sample_size);

    // Build data for sequential read
    let mut set_fid: HashSet<u32> = HashSet::new();
    // Build data for random read
    let mut vec_fid_offset_random: Vec<(u32, u64)> = Vec::new();

    let mut rng: IsaacRng = SeedableRng::from_entropy();

    for item in vec_fid_offset {
        set_fid.insert(item.first().unwrap().0);

        let mut shuffle_vec = item.clone();

        shuffle_vec.shuffle(&mut rng);

        vec_fid_offset_random.append(&mut shuffle_vec);
    }

    let mut iter_set = set_fid.iter().cycle();

    let label_dict = Arc::new(LabelDict::default());

    // Benchmark test for sequential read
    // Fetch an infinite number of elements via a cycle iterator such that the
    // benchmark terminates when the test time is reached
    {
        group
            .sample_size(SEQUENTIAL_READ_SAMPLE_SIZE)
            .throughput(Throughput::Bytes(
                cstore_config.store.mem_segment_size as u64,
            ));

        let parameter = String::from("read_seq_") + u64::to_string(property_len).as_ref();

        // The test code in cur benchmark.
        group.bench_with_input(BenchmarkId::from_parameter(parameter), &(), |b, ()| {
            b.iter(|| {
                let engine_context: Arc<EngineContext> =
                    Arc::new(EngineContext::new(&label_dict, &cstore_config));
                let table_context = TableContext::new(
                    &cstore_config.table,
                    *iter_set.next().unwrap(),
                    &engine_context.file_operator,
                );

                let readable_table = ReadableTable::new(
                    &cstore_config.table,
                    table_context,
                    &engine_context.cache_handler,
                )
                .unwrap();

                let mut sequential_read_iter = SequentialReadIter::new(readable_table);

                loop {
                    match sequential_read_iter.next() {
                        Some(_a) => (),
                        None => {
                            break;
                        }
                    };
                }
            });
        });
    }

    // Benchmark test for random read
    // Fetch an infinite number of elements via a cycle iterator such that the
    // benchmark terminates when the test time is reached
    {
        group.throughput(Throughput::Bytes(*property_len));

        let mut iter_random = vec_fid_offset_random.iter().cycle();
        let parameter = String::from("read_random_") + u64::to_string(property_len).as_ref();
        let mut cur_fid = 0;

        let engine_context: Arc<EngineContext> =
            Arc::new(EngineContext::new(&label_dict, &cstore_config));

        let table_context =
            TableContext::new(&cstore_config.table, 0, &engine_context.file_operator);

        let mut readable_table = ReadableTable::new(
            &cstore_config.table,
            table_context,
            &engine_context.cache_handler,
        )
        .unwrap();

        // The test code in cur benchmark.
        group.bench_with_input(
            BenchmarkId::from_parameter(parameter),
            property_len,
            |b, &_property_len| {
                b.iter(|| {
                    let fid_offset_item = iter_random.next().unwrap();
                    let fid = fid_offset_item.0;
                    let offset = fid_offset_item.1;

                    if fid != cur_fid {
                        let table_context = TableContext::new(
                            &cstore_config.table,
                            fid,
                            &engine_context.file_operator,
                        );
                        readable_table = ReadableTable::new(
                            &cstore_config.table,
                            table_context,
                            &engine_context.cache_handler,
                        )
                        .unwrap();

                        cur_fid = fid;
                    }
                    readable_table.get(offset).unwrap();
                });
            },
        );

        readable_table.close();
    }
}

// Test the read and write performance of the table, each round of reports is
// generated by the property of a certain length.
fn performance_bench_table(c: &mut Criterion) {
    let mut group = c.benchmark_group("bench_table");

    let (cstore_config, graph_data_gen_parameters, bench_parameters) = build_bench_parameters();

    // New a generator and Generate graphdatas of specific amount.
    let mut graph_generator = GraphDataGenerator::new(&graph_data_gen_parameters);
    let graph_data_vec_triad_vec = graph_generator.generate();

    for graph_data_vec_triad in graph_data_vec_triad_vec.iter() {
        let graph_data_vec = &graph_data_vec_triad.2;
        let property_len = graph_data_vec_triad.1 as u64;

        let vec_fid_offset = bench_write_table(
            &cstore_config,
            &bench_parameters,
            &property_len,
            graph_data_vec,
            &mut group,
        );

        bench_read_table(
            &cstore_config,
            &bench_parameters,
            &vec_fid_offset,
            &property_len,
            &mut group,
        );

        // Remove the dir read and written, to make sure tests under different property
        // size be not affected by the file length.
        delete_test_dir(&cstore_config.table, &cstore_config.persistent);
    }

    group.finish();
}

// Entrance of cur benchmark, all the bench functions will be run with command
// `make bench mod=<bench_mod_name>` executed in terminal.
criterion_group!(benches, performance_bench_table);
criterion_main!(benches);
