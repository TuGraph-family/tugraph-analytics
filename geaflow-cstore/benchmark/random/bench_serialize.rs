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

use std::time::Duration;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use flatbuffers::FlatBufferBuilder;
use geaflow_cstore::{
    api::filter::create_empty_filter,
    engine::dict::ttl_dict::TtlDict,
    log_util,
    log_util::{LogLevel, LogType},
    serialize_util::{
        add_second_key_for_flatbuffer, deserialize_index_for_flatbuffer,
        deserialize_index_with_filter_pushdown, serialize_index, serialize_index_for_flatbuffer,
        with_second_key_vec_capacity_for_flatbuffer, SerializeType,
    },
    test_util::{BenchParameters, GraphDataGenerator, GraphDataGeneratorParameters},
    GraphData, GraphDataIndex,
};
use libc::group;

const SERIALIZE_TARGET_ID_NUM_VEC: [usize; 6] = [4, 16, 64, 128, 512, 2048];

fn build_bench_parameters() -> (GraphDataGeneratorParameters, BenchParameters) {
    let graph_data_gen_parameters = GraphDataGeneratorParameters::default()
        .with_target_id_len_vec(vec![4, 16, 64, 128])
        .with_generate_graph_data_num(1000000);

    let bench_parameters = BenchParameters::default();

    (graph_data_gen_parameters, bench_parameters)
}

fn bench_serialize_universal(
    graph_data_gen_parameters: &GraphDataGeneratorParameters,
    group: &mut criterion::BenchmarkGroup<'_, criterion::measurement::WallTime>,
    graph_data_vec: &[(u32, GraphData)],
    serialize_type: SerializeType,
    target_num: &usize,
    target_len: &usize,
) {
    let mut iter = graph_data_vec.iter().cycle();
    let mut index = 0;

    let mut graph_data_index: GraphDataIndex = GraphDataIndex {
        graph_data_fid: 0,
        graph_data_offset: 0,
        all_filtered: false,
        second_keys: vec![],
    };

    let generate_graph_data_num = graph_data_gen_parameters.generate_graph_data_num;

    let mut count = 0;
    let mut second_key_vec_data: Vec<Vec<u8>> = Vec::with_capacity(generate_graph_data_num);

    let parameter = serialize_type.to_string()
        + "_Serialize_target_num_"
        + usize::to_string(target_num).as_ref()
        + "_len_"
        + usize::to_string(target_len).as_ref();

    // Bench serialize.
    group.bench_with_input(
        BenchmarkId::from_parameter(parameter),
        target_len,
        |b, &_target_len| {
            b.iter(|| {
                while target_num > &index {
                    graph_data_index
                        .second_keys
                        .push(iter.next().unwrap().1.second_key.clone());
                    index += 1;
                    if count < generate_graph_data_num {
                        count += 1;
                    }
                }

                let v = serialize_index(serialize_type, &graph_data_index);
                graph_data_index.second_keys.clear();
                index = 0;
                if count < generate_graph_data_num {
                    second_key_vec_data.push(v);
                }
            });
        },
    );

    let mut iter = second_key_vec_data.iter().cycle();

    let parameter = serialize_type.to_string()
        + "_Deserialize_target_num_"
        + usize::to_string(target_num).as_ref()
        + "_len_"
        + usize::to_string(target_len).as_ref();

    let ttl_dict = TtlDict::default();
    // Bench deserialize.
    group.bench_with_input(
        BenchmarkId::from_parameter(parameter),
        target_len,
        |b, &_target_len| {
            b.iter(|| {
                deserialize_index_with_filter_pushdown(
                    serialize_type,
                    iter.next().unwrap(),
                    &create_empty_filter(),
                    &ttl_dict,
                );
            });
        },
    );
}

fn bench_serialize_flatbuffer(
    graph_data_gen_parameters: &GraphDataGeneratorParameters,
    group: &mut criterion::BenchmarkGroup<'_, criterion::measurement::WallTime>,
    graph_data_vec: &[(u32, GraphData)],
    target_num: &usize,
    target_len: &usize,
) {
    let mut iter = graph_data_vec.iter().cycle();

    let mut builder = FlatBufferBuilder::new();
    let mut second_key_vec = with_second_key_vec_capacity_for_flatbuffer(*target_num);

    let mut index = 0;

    let generate_graph_data_num = graph_data_gen_parameters.generate_graph_data_num;

    let mut count = 0;
    let mut second_key_vec_data: Vec<Vec<u8>> = Vec::with_capacity(generate_graph_data_num);

    let parameter = SerializeType::FlatBuffer.to_string()
        + "_Serialize_target_num_"
        + usize::to_string(target_num).as_ref()
        + "_len_"
        + usize::to_string(target_len).as_ref();

    // Bench serialize.
    group.bench_with_input(
        BenchmarkId::from_parameter(parameter),
        target_len,
        |b, &_target_len| {
            b.iter(|| {
                while target_num > &index {
                    let it = &iter.next().unwrap().1.second_key;

                    add_second_key_for_flatbuffer(
                        &mut builder,
                        &mut second_key_vec,
                        it.ts,
                        it.graph_info,
                        it.sequence_id,
                        &it.target_id,
                    );

                    index += 1;

                    if count < generate_graph_data_num {
                        count += 1;
                    }
                }

                let v = serialize_index_for_flatbuffer(&mut builder, &second_key_vec, 0, 0);

                builder.reset();
                second_key_vec.clear();

                index = 0;

                if count < generate_graph_data_num {
                    second_key_vec_data.push(v);
                }
            });
        },
    );

    let mut iter = second_key_vec_data.iter().cycle();

    let parameter = SerializeType::FlatBuffer.to_string()
        + "_Deserialize_target_num_"
        + usize::to_string(target_num).as_ref()
        + "_len_"
        + usize::to_string(target_len).as_ref();

    // Bench deserialize.
    group.bench_with_input(
        BenchmarkId::from_parameter(parameter),
        target_len,
        |b, &_target_len| {
            b.iter(|| {
                deserialize_index_for_flatbuffer(iter.next().unwrap());
            });
        },
    );
}

fn criterion_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("bench_serialize");

    let (graph_data_gen_parameters, bench_parameters) = build_bench_parameters();

    // New a generator and Generate graphdatas of specific amount.z
    let mut graph_generator = GraphDataGenerator::new(&graph_data_gen_parameters);
    let graph_data_vec_triad_vec = graph_generator.generate();

    for graph_data_vec_triad in graph_data_vec_triad_vec.iter() {
        for target_id_num in SERIALIZE_TARGET_ID_NUM_VEC.iter() {
            println!(
                "start new round test, target len {}",
                graph_data_vec_triad.0
            );

            // Set the iteration time of the benchmark by the length of the property, to
            // ensure that the number of iterations under each property exceeds one
            // million times, and effective samples of sample size specified are taken.
            group
                .measurement_time(Duration::from_secs(
                    bench_parameters.default_bench_measure_time,
                ))
                .sample_size(bench_parameters.sample_size);

            bench_serialize_universal(
                &graph_data_gen_parameters,
                &mut group,
                &graph_data_vec_triad.2,
                SerializeType::ISerde,
                target_id_num,
                &graph_data_vec_triad.0,
            );

            bench_serialize_universal(
                &graph_data_gen_parameters,
                &mut group,
                &graph_data_vec_triad.2,
                SerializeType::Serde,
                target_id_num,
                &graph_data_vec_triad.0,
            );

            bench_serialize_universal(
                &graph_data_gen_parameters,
                &mut group,
                &graph_data_vec_triad.2,
                SerializeType::BinCode,
                target_id_num,
                &graph_data_vec_triad.0,
            );

            bench_serialize_flatbuffer(
                &graph_data_gen_parameters,
                &mut group,
                &graph_data_vec_triad.2,
                target_id_num,
                &graph_data_vec_triad.0,
            )
        }
    }

    group.finish();
}

// Entrance of cur benchmark, all the bench functions will be run with command
// `make bench mod=<bench_mod_name>` executed in terminal.
criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
