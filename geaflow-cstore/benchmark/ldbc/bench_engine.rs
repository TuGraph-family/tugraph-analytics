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

use std::{
    collections::HashSet,
    ops::Deref,
    sync::Arc,
    time::{Duration, Instant},
};

use criterion::{criterion_group, criterion_main, Criterion};
use geaflow_cstore::{
    api::graph::{edge::Edge, vertex::Vertex, EdgeDirection},
    test_util::{bind_job_name_with_ts, delete_test_dir, BenchParameters},
    CStore, CStoreConfig, CStoreConfigBuilder, ConfigMap, Direct, GraphStruct, LdbcSource,
};
use prost::Message;
use rand::{prelude::SliceRandom, thread_rng};
use rustc_hash::FxHashMap;

fn build_bench_parameters() -> (CStoreConfig, BenchParameters) {
    let mut config_map = ConfigMap::default();

    let job_name = bind_job_name_with_ts("bench_engine_with_ldbc");
    config_map.insert("store.job_name", job_name.as_str());
    config_map.insert("log.enable_log", "false");

    let cstore_config = CStoreConfigBuilder::default()
        .set_with_map(&config_map)
        .build();

    let bench_parameters = BenchParameters::default()
        .with_default_bench_measure_time(60)
        .with_sample_size(10)
        .with_read_bench_measure_time(30);

    (cstore_config, bench_parameters)
}

fn insert_data(data: &GraphStruct, cstore: &mut CStore, ids: &mut HashSet<u32>) {
    match data {
        GraphStruct::Edge(edge) => {
            cstore.add_edge(Edge::create_id_time_label_edge(
                edge.sid.encode_to_vec(),
                edge.tid.encode_to_vec(),
                edge.ts,
                edge.label.deref().clone(),
                match edge.direct {
                    Direct::Out => EdgeDirection::Out,
                    Direct::In => EdgeDirection::In,
                },
                edge.property.deref().clone(),
            ));

            ids.insert(edge.sid as u32);
        }
        GraphStruct::BothEdge(out_edge, in_edge) => {
            cstore.add_edge(Edge::create_id_time_label_edge(
                out_edge.sid.encode_to_vec(),
                out_edge.tid.encode_to_vec(),
                out_edge.ts,
                out_edge.label.deref().clone(),
                match out_edge.direct {
                    Direct::Out => EdgeDirection::Out,
                    Direct::In => EdgeDirection::In,
                },
                out_edge.property.deref().clone(),
            ));

            cstore.add_edge(Edge::create_id_time_label_edge(
                in_edge.sid.encode_to_vec(),
                in_edge.tid.encode_to_vec(),
                in_edge.ts,
                in_edge.label.deref().clone(),
                match in_edge.direct {
                    Direct::Out => EdgeDirection::Out,
                    Direct::In => EdgeDirection::In,
                },
                in_edge.property.deref().clone(),
            ));

            ids.insert(out_edge.sid as u32);
        }
        GraphStruct::Vertex(vertex) => {
            cstore.add_vertex(Vertex::create_id_time_label_vertex(
                vertex.id.encode_to_vec(),
                vertex.ts,
                vertex.label.deref().clone(),
                vertex.property.deref().clone(),
            ));

            ids.insert(vertex.id as u32);
        }
        GraphStruct::None => {}
    }
}

/// download http://alipay-kepler-resource.cn-hangzhou.alipay.aliyun-inc.com/sf0.1.tar and untar
fn write_benchmark(c: &mut Criterion) {
    // Load the ldbc source
    let mut ldbc = FxHashMap::default();
    ldbc.insert(String::from("local.ldbc.path"), String::from("/tmp/sf0.1"));
    let ldbc_source = LdbcSource::new(&ldbc);

    let (cstore_config, bench_parameters) = build_bench_parameters();

    // Set the iteration time of the benchmark by the length of the property, to
    // ensure that the number of iterations under each property exceeds one
    // thousand times, and effective samples of sample size specified are taken.
    let mut group = c.benchmark_group("bench_ldbc_store_write");
    group
        .measurement_time(Duration::from_secs(
            bench_parameters.default_bench_measure_time,
        ))
        .sample_size(bench_parameters.sample_size);

    // New engine with cstore config
    let table_config = Arc::clone(&cstore_config.table);
    let persistent_config = cstore_config.persistent.clone();
    let mut store = CStore::new(cstore_config);

    // The test code in cur benchmark.
    group.bench_function("sf0.1", |b| {
        b.iter_custom(|iters| {
            let mut ids: HashSet<u32> = HashSet::new();
            let mut data_iter = ldbc_source.get_iter();
            let start = Instant::now();
            for _i in 0..iters {
                let next = data_iter.next();
                if next.is_some() {
                    insert_data(&next.unwrap(), &mut store, &mut ids);
                } else {
                    data_iter = ldbc_source.get_iter();
                    let next = data_iter.next();
                    insert_data(&next.unwrap(), &mut store, &mut ids);
                }
            }
            store.flush();

            ids.clear();
            start.elapsed()
        });
    });

    store.close();

    delete_test_dir(&table_config, &persistent_config);

    group.finish();
}

fn read_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("bench_ldbc_store_read");

    // Load the ldbc source
    let mut ldbc = FxHashMap::default();
    ldbc.insert(String::from("local.ldbc.path"), String::from("/tmp/sf0.1"));
    let ldbc_source = LdbcSource::new(&ldbc);

    let (cstore_config, bench_parameters) = build_bench_parameters();

    let table_config = Arc::clone(&cstore_config.table);
    let persistent_config = cstore_config.persistent.clone();
    // Set the iteration time of the benchmark by the length of the property, to
    // ensure that the number of iterations under each property exceeds one
    // thousand times, and effective samples of sample size specified are taken.
    group
        .measurement_time(Duration::from_secs(
            bench_parameters.read_bench_measure_time,
        ))
        .sample_size(bench_parameters.sample_size);

    // New engine engine with cstore config
    let mut store = CStore::new(cstore_config);

    let mut ids: HashSet<u32> = HashSet::new();
    for data in ldbc_source.get_iter() {
        insert_data(&data, &mut store, &mut ids);
    }
    store.flush();

    let mut id_vec: Vec<_> = ids.into_iter().collect();
    id_vec.shuffle(&mut thread_rng());
    let mut id_iter = id_vec.into_iter().cycle();

    // The test code in cur benchmark.
    group.bench_function("sf0.1", |b| {
        b.iter(|| {
            let id_opt = id_iter.next();
            store.get_vertex_and_edge(&id_opt.unwrap().encode_to_vec());
        })
    });

    store.close();

    // Remove the dir read and written, to make sure tests under different property
    // size be not affected by the file length.
    delete_test_dir(&table_config, &persistent_config);

    group.finish();
}

fn scan_vertex_and_edge_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("bench_ldbc_store_scan_vertex_and_edge");

    // Load the ldbc source
    let mut ldbc = FxHashMap::default();
    ldbc.insert(String::from("local.ldbc.path"), String::from("/tmp/sf0.1"));
    let ldbc_source = LdbcSource::new(&ldbc);

    let (cstore_config, bench_parameters) = build_bench_parameters();

    let table_config = cstore_config.table.clone();
    let persistent_config = cstore_config.persistent.clone();
    // Set the iteration time of the benchmark by the length of the property, to
    // ensure that the number of iterations under each property exceeds one
    // thousand times, and effective samples of sample size specified are taken.
    group
        .measurement_time(Duration::from_secs(
            bench_parameters.read_bench_measure_time,
        ))
        .sample_size(bench_parameters.sample_size);

    // New engine engine with cstore config
    let mut store = CStore::new(cstore_config);

    let mut ids: HashSet<u32> = HashSet::new();
    for data in ldbc_source.get_iter() {
        insert_data(&data, &mut store, &mut ids);
    }
    store.flush();

    let mut iter = store.scan_vertex_and_edge();
    // The test code in cur benchmark.
    group.bench_function("sf0.1", |b| {
        b.iter(|| {
            let next_opt = iter.next();
            if next_opt.is_none() {
                iter = store.scan_vertex_and_edge();
            } else {
                let _vertex_and_edge = next_opt.unwrap();
            }
        })
    });

    store.close();

    // Remove the dir read and written, to make sure tests under different property
    // size be not affected by the file length.
    delete_test_dir(&table_config, &persistent_config);

    group.finish();
}

fn scan_edge_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("bench_ldbc_store_scan_edge");

    // Load the ldbc source
    let mut ldbc = FxHashMap::default();
    ldbc.insert(String::from("local.ldbc.path"), String::from("/tmp/sf0.1"));
    let ldbc_source = LdbcSource::new(&ldbc);

    let (cstore_config, bench_parameters) = build_bench_parameters();

    let table_config = cstore_config.table.clone();
    let persistent_config = cstore_config.persistent.clone();
    // Set the iteration time of the benchmark by the length of the property, to
    // ensure that the number of iterations under each property exceeds one
    // thousand times, and effective samples of sample size specified are taken.
    group
        .measurement_time(Duration::from_secs(
            bench_parameters.read_bench_measure_time,
        ))
        .sample_size(bench_parameters.sample_size);

    // New engine engine with cstore config
    let mut store = CStore::new(cstore_config);

    let mut ids: HashSet<u32> = HashSet::new();
    for data in ldbc_source.get_iter() {
        insert_data(&data, &mut store, &mut ids);
    }
    store.flush();

    let mut iter = store.scan_edge();
    // The test code in cur benchmark.
    group.bench_function("sf0.1", |b| {
        b.iter(|| {
            let next_opt = iter.next();
            if next_opt.is_none() {
                iter = store.scan_edge();
            } else {
                let _edge = next_opt.unwrap();
            }
        })
    });

    store.close();

    // Remove the dir read and written, to make sure tests under different property
    // size be not affected by the file length.
    delete_test_dir(&table_config, &persistent_config);

    group.finish();
}

fn benchmark(c: &mut Criterion) {
    write_benchmark(c);
    read_benchmark(c);
    scan_vertex_and_edge_benchmark(c);
    scan_edge_benchmark(c);
}

// Entrance of cur benchmark, all the bench functions will be run with command
// `make bench mod=<bench_mod_name>` executed in terminal.
criterion_group!(benches, benchmark);
criterion_main!(benches);
