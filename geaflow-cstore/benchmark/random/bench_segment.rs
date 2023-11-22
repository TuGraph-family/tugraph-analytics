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
    engine::dict::label_dict::LabelDict,
    test_util::{BenchParameters, GraphDataGenerator, GraphDataGeneratorParameters},
    CStoreConfig, EngineContext, GraphSegment,
};

fn build_bench_parameters() -> (GraphDataGeneratorParameters, BenchParameters) {
    let graph_data_gen_parameters =
        GraphDataGeneratorParameters::default().with_generate_graph_data_num(1000000);

    let bench_parameters = BenchParameters::default();

    (graph_data_gen_parameters, bench_parameters)
}

fn criterion_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("bench_segment");

    let cstore_config = CStoreConfig::default();
    let (graph_data_gen_parameters, bench_parameters) = build_bench_parameters();
    let label_dict = Arc::new(LabelDict::default());

    // The length of the graphdata's property in cur benchmark is 4, 16, 64...
    let mut graph_segment = GraphSegment::new(&cstore_config.store);
    let engine_context = EngineContext::new(&label_dict, &cstore_config);

    // New a generator and Generate graphdatas of specific amount.
    let mut graph_generator = GraphDataGenerator::new(&graph_data_gen_parameters);
    let graph_data_vec_triad_vec = graph_generator.generate();

    for graph_data_vec_triad in graph_data_vec_triad_vec.iter() {
        let property_len = graph_data_vec_triad.1 as u64;

        // New a generator and Generate graphdatas of specific amount.

        // Set the iteration time of the benchmark by the length of the property, to
        // ensure that the number of iterations under each property exceeds one
        // thousand times, and effective samples of sample size specified are taken.
        group
            .throughput(Throughput::Bytes(property_len))
            .measurement_time(Duration::from_secs(
                bench_parameters.write_bench_measure_time,
            ))
            .sample_size(bench_parameters.sample_size);

        // Fetch an infinite number of elements via a cycle iterator such that the
        // benchmark terminates when the test time is reached
        let mut iter = graph_data_vec_triad.2.iter().cycle();

        group.bench_with_input(BenchmarkId::from_parameter(property_len), &(), |b, ()| {
            b.iter(|| {
                graph_segment.put(iter.next().unwrap().0, iter.next().unwrap().1.clone());

                if graph_segment.need_freeze() {
                    let graph_segment_view = graph_segment.view(&engine_context);
                    for _graph_data in graph_segment_view {
                        // empty loop.
                    }

                    graph_segment.close();
                }
            });
        });

        // Clear the data in segment.
        graph_segment.close();
    }
    group.finish();
}

// Entrance of cur benchmark, all the bench functions will be run with command
// `make bench mod=<bench_mod_name>` executed in terminal.
criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
