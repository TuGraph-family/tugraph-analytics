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
    collections::{HashMap, HashSet},
    path::Path,
    sync::Arc,
};

use itertools::Itertools;
use prost::Message;
use rand::prelude::*;
use rand_isaac::isaac::IsaacRng;
use sha2::{Digest, Sha256};

use super::thread_util::RayonThreadPool;
use crate::{
    api::{
        filter::create_empty_filter_pushdown,
        graph::{
            edge::{self, Edge},
            graph_info_util::is_vertex,
            graph_serde::GraphSerde,
            vertex::Vertex,
            EdgeDirection,
        },
        iterator::{edge_iterator::EdgeIter, vertex_iterator::VertexIter},
    },
    cache::CStoreCacheHandler,
    common::GraphData,
    config::{ManifestConfig, PersistentConfig, TableConfig},
    engine::dict::label_dict::LabelDict,
    file::{
        file_handle::LocalBufferReader,
        file_operator::{CstoreFileOperator, FileOperator, LocalFileOperator},
    },
    index::parent_node::IndexMeta,
    log_util::info,
    s_info,
    time_util::time_now_as_ns,
    with_bound, CStore, CStoreConfig, CStoreConfigBuilder, ConfigMap, Direct, Engine, GraphStruct,
    LdbcEdge, ReadableTable, Result, SequentialReadIter, TableContext, WritableTable,
    FILE_PATH_DELIMITER,
};

// Others Begin
pub const READ_BUFFER_LEN: usize = 1024;

type GraphDataVecTriadVec = Vec<(usize, usize, Vec<(u32, GraphData)>)>;

type FidOffsetDataPropertyVec<'a> = Vec<Vec<(u32, u64, Vec<&'a [u8]>)>>;

pub struct GraphDataGeneratorParameters {
    pub random_switch: bool,
    pub property_len_vec: Vec<usize>,
    pub property_len_interval_vec: Vec<(usize, usize)>,
    pub target_id_len_vec: Vec<usize>,
    pub target_id_len_interval_vec: Vec<(usize, usize)>,
    pub degree: usize,
    pub generate_graph_data_num: usize,
}

impl Default for GraphDataGeneratorParameters {
    fn default() -> Self {
        Self {
            random_switch: false,
            property_len_vec: vec![4, 16, 64, 128, 512, 2048],
            property_len_interval_vec: vec![
                (0, 1),
                (1, 50),
                (50, 100),
                (100, 1000),
                (1000, 5000),
                (5000, 25000),
            ],
            target_id_len_vec: vec![4],
            target_id_len_interval_vec: vec![(0, 4), (4, 16), (16, 48)],
            degree: 4,
            generate_graph_data_num: 10000,
        }
    }
}

with_bound!(GraphDataGeneratorParameters, self, random_switch: bool,
            property_len_vec: Vec<usize>, property_len_interval_vec: Vec<(usize, usize)>,
            target_id_len_vec: Vec<usize>, target_id_len_interval_vec: Vec<(usize, usize)>,
            degree: usize, generate_graph_data_num: usize);

/// A random generator of graph data.
/// s
/// Every iteration should be taken in µs.
pub struct GraphDataGenerator {
    random_switch: bool,

    property_len_vec: Vec<usize>,

    property_len: usize,

    property_len_interval_vec: Vec<(usize, usize)>,

    property_len_interval: (usize, usize),

    target_id_len_vec: Vec<usize>,

    target_id_len: usize,

    target_id_len_interval_vec: Vec<(usize, usize)>,

    target_id_len_interval: (usize, usize),

    degree: usize,

    generate_graph_data_num: usize,

    rng: IsaacRng,

    graph_data_vec: Vec<GraphData>,

    src_id: u32,

    graph_serde: GraphSerde,

    label_dict: Arc<LabelDict>,
}

impl GraphDataGenerator {
    pub fn new(parameters: &GraphDataGeneratorParameters) -> Self {
        GraphDataGenerator {
            random_switch: parameters.random_switch,
            property_len_vec: parameters.property_len_vec.clone(),
            property_len: 0,
            property_len_interval_vec: parameters.property_len_interval_vec.clone(),
            property_len_interval: (0, 0),
            target_id_len_vec: parameters.target_id_len_vec.clone(),
            target_id_len: 0,
            target_id_len_interval_vec: parameters.target_id_len_interval_vec.clone(),
            target_id_len_interval: (0, 0),
            degree: parameters.degree,
            generate_graph_data_num: parameters.generate_graph_data_num,
            rng: SeedableRng::from_entropy(),
            graph_data_vec: vec![],
            src_id: 0,
            graph_serde: GraphSerde::default(),
            label_dict: Arc::new(LabelDict::default()),
        }
    }

    pub fn with_seed(mut self, seed: u64) -> Self {
        self.rng = IsaacRng::seed_from_u64(seed);
        self
    }

    pub fn generate(&mut self) -> GraphDataVecTriadVec {
        let mut data_vec: GraphDataVecTriadVec = Vec::new();
        let target_id_len_vec = self.target_id_len_vec.clone();
        let property_len_vec = self.property_len_vec.clone();
        let target_id_len_interval_vec = self.target_id_len_interval_vec.clone();
        let property_len_interval_vec = self.property_len_interval_vec.clone();
        let generate_graph_data_num = self.generate_graph_data_num;

        match self.random_switch {
            false => {
                for i in &target_id_len_vec {
                    for j in &property_len_vec {
                        self.target_id_len = *i;
                        self.property_len = *j;

                        let tmp_vec: (usize, usize, Vec<(u32, GraphData)>) =
                            (*i, *j, self.take(generate_graph_data_num).collect());

                        data_vec.push(tmp_vec);
                    }
                }
            }
            true => {
                for i in &target_id_len_interval_vec {
                    for j in &property_len_interval_vec {
                        self.target_id_len_interval = *i;
                        self.property_len_interval = *j;

                        let tmp_vec: (usize, usize, Vec<(u32, GraphData)>) =
                            (i.0, j.0, self.take(generate_graph_data_num).collect());

                        data_vec.push(tmp_vec);
                    }
                }
            }
        }
        data_vec
    }

    fn build_next_graph(&mut self) {
        self.src_id = self.rng.next_u32();

        for _i in 0..self.degree {
            let target_id_len = match self.random_switch {
                false => self.target_id_len,

                true => self
                    .rng
                    .gen_range(self.target_id_len_interval.0..self.target_id_len_interval.1),
            };

            let mut target_id = vec![0; target_id_len];
            self.rng.fill_bytes(&mut target_id);

            let property_len = match self.random_switch {
                false => self.property_len,

                true => self
                    .rng
                    .gen_range(self.property_len_interval.0..self.property_len_interval.1),
            };

            let mut property = vec![0; property_len];

            self.rng.fill_bytes(&mut property);

            let edge = Edge::create_id_edge(
                self.src_id.to_be_bytes().to_vec(),
                target_id,
                EdgeDirection::Out,
                property,
            );

            self.graph_data_vec
                .push(self.graph_serde.serialize_edge(&edge, &self.label_dict));
        }
    }
}

impl Iterator for GraphDataGenerator {
    type Item = (u32, GraphData);

    fn next(&mut self) -> Option<(u32, GraphData)> {
        if self.graph_data_vec.is_empty() {
            self.build_next_graph();
        }

        Some((self.src_id, self.graph_data_vec.pop().unwrap()))
    }
}

pub struct MemIndexGenerator {
    random_switch: bool,

    key_interval: (u32, u32),

    fid_interval: (u32, u32),

    offset_interval: (u64, u64),

    rng: IsaacRng,

    pub mem_index_value: Vec<(u32, IndexMeta)>,
}

pub struct MemIndexGeneratorParameters {
    pub random_switch: bool,
    pub key_interval: (u32, u32),
    pub fid_interval: (u32, u32),
    pub offset_interval: (u64, u64),
    pub thread_num: u32,
}

impl Default for MemIndexGeneratorParameters {
    fn default() -> Self {
        Self {
            random_switch: false,
            key_interval: (0, 13107200),
            fid_interval: (0, 1024),
            offset_interval: (0, 1024),
            thread_num: 5,
        }
    }
}

with_bound!(MemIndexGeneratorParameters, self, random_switch: bool, key_interval: (u32, u32),
            fid_interval: (u32, u32), offset_interval: (u64, u64), thread_num: u32);

impl MemIndexGenerator {
    pub fn new(parameters: &MemIndexGeneratorParameters) -> Self {
        let mut gen = MemIndexGenerator {
            random_switch: parameters.random_switch,
            key_interval: parameters.key_interval,
            fid_interval: parameters.fid_interval,
            offset_interval: parameters.offset_interval,
            rng: SeedableRng::from_entropy(),
            mem_index_value: Vec::new(),
        };

        match gen.random_switch {
            true => {
                for _k in gen.key_interval.0..gen.key_interval.1 {
                    gen.mem_index_value.push((
                        gen.rng.gen_range(gen.key_interval.0..gen.key_interval.1),
                        IndexMeta {
                            fid: gen.rng.gen_range(gen.fid_interval.0..gen.fid_interval.1),
                            offset: gen
                                .rng
                                .gen_range(gen.offset_interval.0..gen.offset_interval.1),
                        },
                    ));
                }
            }

            false => {
                for k in gen.key_interval.0..gen.key_interval.1 {
                    gen.mem_index_value.push((
                        k,
                        IndexMeta {
                            fid: gen.rng.gen_range(gen.fid_interval.0..gen.fid_interval.1),
                            offset: gen
                                .rng
                                .gen_range(gen.offset_interval.0..gen.offset_interval.1),
                        },
                    ));
                }
            }
        }

        gen
    }
}
pub struct TableTestParameters {
    pub multi_read_thread_num: usize,
    pub max_loop_exec_times: usize,
    pub iterations_vec: Vec<usize>,
    pub flush_table_size: u64,
    pub property_num_vec: Vec<u32>,
    pub property_num_default: u32,
}

impl Default for TableTestParameters {
    fn default() -> Self {
        Self {
            multi_read_thread_num: 8,
            max_loop_exec_times: 500000,
            iterations_vec: vec![10, 100, 500],
            flush_table_size: 268435456,
            property_num_vec: vec![4, 12, 16],
            property_num_default: 5,
        }
    }
}

with_bound!(TableTestParameters, self, multi_read_thread_num: usize, max_loop_exec_times: usize,
            iterations_vec: Vec<usize>, flush_table_size: u64, property_num_vec: Vec<u32>);

pub struct BenchParameters {
    pub sample_size: usize,
    pub default_bench_measure_time: u64,
    pub write_bench_measure_time: u64,
    pub read_bench_measure_time: u64,
    pub bench_vec_max_size: usize,
}

impl Default for BenchParameters {
    fn default() -> Self {
        Self {
            sample_size: 100,
            default_bench_measure_time: 5,
            write_bench_measure_time: 5,
            read_bench_measure_time: 5,
            bench_vec_max_size: 50000000,
        }
    }
}

with_bound!(BenchParameters, self, sample_size: usize, default_bench_measure_time: u64,
            write_bench_measure_time: u64, read_bench_measure_time: u64, bench_vec_max_size: usize);

// Test table write, return a Vec<triple>, which describes [fid, block offset,
// reference of block data] for each item.
pub fn test_table_append<'a>(
    table_config: &TableConfig,
    graph_data_vec: &'a [(u32, GraphData)],
    property_vec_num: u32,
    iterations: usize,
    flush_table_size: u64,
    file_operator: &CstoreFileOperator,
) -> FidOffsetDataPropertyVec<'a> {
    let table_context = TableContext::new(table_config, 0, file_operator);
    let mut writable_table = WritableTable::new(table_config, table_context).unwrap();

    // Fetch an infinite number of elements via a cycle iterator.
    let mut iter = graph_data_vec.iter().cycle();

    let mut vec_fid_offset: FidOffsetDataPropertyVec = Vec::new();
    vec_fid_offset.push(Vec::new());

    let mut fid: u32 = 0;

    for _i in 0..iterations {
        let mut property_vec: Vec<&[u8]> = Vec::new();

        let offset = if property_vec_num == 1 {
            let data = &iter.next().unwrap().1.property;
            property_vec.push(data);
            writable_table.append(data).unwrap()
        } else {
            for _j in 0..property_vec_num {
                property_vec.push(&iter.next().unwrap().1.property);
            }
            writable_table.append_list(&property_vec).unwrap()
        };

        vec_fid_offset
            .get_mut(fid as usize)
            .unwrap()
            .push((fid, offset, property_vec));

        if writable_table.get_table_offset() / flush_table_size > 0 {
            vec_fid_offset.push(Vec::new());

            writable_table.finish().unwrap();
            writable_table.close();

            fid += 1;

            writable_table = WritableTable::new(
                table_config,
                TableContext::new(table_config, fid, file_operator),
            )
            .unwrap();
        }
    }

    if vec_fid_offset.last().unwrap().is_empty() {
        vec_fid_offset.pop();
    }

    writable_table.finish().unwrap();
    writable_table.close();

    vec_fid_offset
}

// Test table read randomly.
pub fn test_table_read_randomly(
    table_config: &TableConfig,
    cache_handler: &CStoreCacheHandler,
    vec_fid_offset: &FidOffsetDataPropertyVec,
    table_read_multi_threads_num: usize,
    file_operator: &CstoreFileOperator,
) {
    let thread_pool = RayonThreadPool::new(table_read_multi_threads_num);

    // Build data for random read
    let mut vec_fid_offset_random: Vec<(u32, u64, Vec<&[u8]>)> = Vec::new();

    let mut rng: IsaacRng = SeedableRng::from_entropy();

    // Build the data for read test, include fid vector used to read sequentially
    // and origin data used to check correctness.
    for item in vec_fid_offset {
        // build data for random read.
        let mut shuffle_vec = item.clone();
        shuffle_vec.shuffle(&mut rng);
        vec_fid_offset_random.append(&mut shuffle_vec);
    }

    // Test multithreaded random read
    thread_pool.iter_execute(&vec_fid_offset_random, |i: &(u32, u64, Vec<&[u8]>)| {
        let mut readable_table = ReadableTable::new(
            table_config,
            TableContext::new(table_config, i.0, file_operator),
            cache_handler,
        )
        .unwrap();
        let data = readable_table.get(i.1).unwrap();

        let data_compare: Vec<&[u8]> = data.iter().map(|a| a.as_slice()).collect_vec();
        // Check the correctness of property.
        assert_eq!(data_compare, i.2);
        readable_table.close();
    });
}

// Test table read sequentially.
pub fn test_table_read_sequentially(
    table_config: &TableConfig,
    cache_handler: &CStoreCacheHandler,
    vec_fid_offset: &FidOffsetDataPropertyVec,
    file_operator: &CstoreFileOperator,
) {
    let table_context = TableContext::new(table_config, 0, file_operator);

    // Build data for sequential read
    let mut vec_fid: Vec<u32> = Vec::new();
    let mut vec_fid_offset_sequential: Vec<(u32, u64, Vec<&[u8]>)> = Vec::new();

    // Build the data for read test, include fid vector used to read sequentially
    // and origin data used to check correctness.
    for item in vec_fid_offset {
        // build data for sequential read.
        vec_fid.push(item.first().unwrap().0);
        vec_fid_offset_sequential.append(&mut item.clone());
    }

    let mut iter_fid_vec = vec_fid.iter();
    iter_fid_vec.next();

    let mut readable_table =
        ReadableTable::new(table_config, table_context, cache_handler).unwrap();

    // New a sequentially readable iterator.
    let mut sequential_read_iter = SequentialReadIter::new(readable_table);

    let mut counter = 0;

    loop {
        let data: Option<Vec<Vec<u8>>> = sequential_read_iter.next();
        match data {
            Some(data) => {
                // Check the correctness of property.
                let data_compare: Vec<&[u8]> = data.iter().map(|a| a.as_slice()).collect_vec();

                assert_eq!(
                    data_compare,
                    vec_fid_offset_sequential.get(counter).unwrap().2
                );
                counter += 1;
            }

            None => {
                // Upgrade the fid.

                match iter_fid_vec.next() {
                    Some(fid) => {
                        let table_context = TableContext::new(table_config, *fid, file_operator);
                        readable_table =
                            ReadableTable::new(table_config, table_context, cache_handler).unwrap();

                        sequential_read_iter = SequentialReadIter::new(readable_table);
                    }

                    None => break,
                }
                // Reset the iterator and reopen with next file.
            }
        }
    }
}

pub fn gen_random_data(buffer: &mut [u8]) {
    let mut rng: IsaacRng = SeedableRng::from_entropy();
    rng.fill_bytes(buffer);
}

pub fn files_are_equal(file1_path: &Path, file2_path: &Path) -> Result<bool> {
    let mut file1 = LocalBufferReader::new(file1_path)?;
    let mut file2 = LocalBufferReader::new(file2_path)?;

    let mut buffer1 = [0; READ_BUFFER_LEN];
    let mut buffer2 = [0; READ_BUFFER_LEN];

    let mut hasher1 = Sha256::new();
    let mut hasher2 = Sha256::new();

    loop {
        let bytes_read1 = file1.read(&mut buffer1)?;
        let _bytes_read2 = file2.read(&mut buffer2)?;

        hasher1.update(buffer1.as_slice());
        hasher2.update(buffer2.as_slice());

        if bytes_read1 == 0 {
            break;
        }
    }

    Ok(hasher1.finalize() == hasher2.finalize())
}

pub fn bytes_are_equal(buffer1: &[u8], buffer2: &[u8]) -> bool {
    let mut hasher1 = Sha256::new();
    let mut hasher2 = Sha256::new();

    hasher1.update(buffer1);
    hasher2.update(buffer2);

    hasher1.finalize() == hasher2.finalize()
}

pub fn get_drop_path(name_space: &str) -> &Path {
    let mut drop_dir_path_str = name_space;
    let mut fix_len = 0;
    for _i in 0..2 {
        if let Some(pos) = drop_dir_path_str.rfind(FILE_PATH_DELIMITER) {
            fix_len += drop_dir_path_str.len() - pos;
            drop_dir_path_str = &drop_dir_path_str[..pos];
        }
    }

    Path::new(&name_space[..name_space.len() - fix_len])
}

pub fn delete_test_dir(table_config: &TableConfig, persistent_config: &PersistentConfig) {
    let file_operator = CstoreFileOperator::new(persistent_config).unwrap();

    let file_handle_type = &table_config.base.file_handle_type;
    let drop_name_space = if file_handle_type.is_local_mode() {
        &table_config.base.local_name_space
    } else {
        &table_config.base.persistent_name_space
    };

    file_operator
        .remove_path(file_handle_type, get_drop_path(drop_name_space))
        .unwrap();
}

pub fn delete_fo_test_dir(manifest_config: &ManifestConfig, persistent_config: &PersistentConfig) {
    let file_operator = CstoreFileOperator::new(persistent_config).unwrap();

    LocalFileOperator
        .remove_path(get_drop_path(&manifest_config.base.local_name_space))
        .unwrap();

    let file_handle_type = &manifest_config.base.persistent_file_handle_type;
    file_operator
        .remove_path(
            file_handle_type,
            get_drop_path(&manifest_config.base.persistent_name_space),
        )
        .unwrap();
}

pub fn bind_job_name_with_ts(job_name: &str) -> String {
    let mut job_name = String::from(job_name);
    job_name.push_str(time_now_as_ns().to_string().as_str());
    job_name
}

pub fn vertex_is_equal(vertex1: Vertex, vertex2: Vertex) -> bool {
    bytes_are_equal(vertex1.property(), vertex2.property())
        && vertex1.ts().eq(&vertex2.ts())
        && vertex1.label().eq(vertex2.label())
}

pub fn edge_is_equal(edge1: &Edge, edge2: &Edge) -> bool {
    bytes_are_equal(edge1.target_id(), edge2.target_id())
        && edge1.ts().eq(&edge2.ts())
        && edge1.label().eq(edge2.label())
        && edge1.direction().eq(edge2.direction())
        && bytes_are_equal(edge1.property(), edge2.property())
}

pub fn vertex_iter_is_equal(
    vertex_iter_1: &mut VertexIter,
    vertex_iter_2: &mut VertexIter,
) -> bool {
    loop {
        let vertex1 = vertex_iter_1.next();
        let vertex2 = vertex_iter_2.next();

        if (vertex1.is_none() && vertex2.is_some()) || (vertex1.is_some() && vertex2.is_none()) {
            return false;
        } else if vertex1.is_some() && vertex2.is_some() {
            if !vertex_is_equal(vertex1.unwrap(), vertex2.unwrap()) {
                return false;
            }
        } else {
            return true;
        }
    }
}

pub fn edge_iter_is_equal(edge_iter_1: &mut EdgeIter, edge_iter_2: &mut EdgeIter) -> bool {
    loop {
        let edge1 = edge_iter_1.next();
        let edge2 = edge_iter_2.next();

        if (edge1.is_none() && edge2.is_some()) || (edge1.is_some() && edge2.is_none()) {
            return false;
        } else if edge1.is_some() && edge2.is_some() {
            if !edge_is_equal(&edge1.unwrap(), &edge2.unwrap()) {
                return false;
            }
        } else {
            return true;
        }
    }
}

// uncomment the config below to enable all oss tests.
pub fn build_oss_test_config(_config_map: &mut ConfigMap) {
    // _config_map.insert("store.location", "Remote");
    //
    // _config_map.insert("persistent.persistent_type", "Oss");
    // _config_map.insert("persistent.root", "/geaflow/chk/cstore_test/");
    // _config_map.insert("persistent.config.endpoint", "");
    // _config_map.insert("persistent.config.bucket", "");
    // _config_map.insert("persistent.config.access_key_id", "");
    // _config_map.insert("persistent.config.access_key_secret", "");
}

pub fn build_oss_persistent_config(test_name: &str) -> PersistentConfig {
    let mut config_map = ConfigMap::default();

    let job_name = bind_job_name_with_ts(test_name);
    config_map.insert("store.job_name", job_name.as_str());
    build_oss_test_config(&mut config_map);

    let cstore_config: CStoreConfig = CStoreConfigBuilder::default()
        .set_with_map(&config_map)
        .build();

    cstore_config.persistent.clone()
}

#[cfg(feature = "hdfs")]
pub fn build_hdfs_test_config(_config_map: &mut ConfigMap) {
    // _config_map.insert("store.location", "Remote");
    // _config_map.insert("persistent.persistent_type", "Hdfs");
    // _config_map.insert("persistent.root", "/geaflow/chk/cstore_test/");
    // _config_map.insert("persistent.config.name_node", "hdfs://localhost:9000");
    // _config_map.insert("persistent.config.enable_append", "true");
}

#[cfg(feature = "hdfs")]
pub fn build_hdfs_persistent_config(test_name: &str) -> PersistentConfig {
    let mut config_map = ConfigMap::default();

    let job_name = bind_job_name_with_ts(test_name);
    config_map.insert("store.job_name", job_name.as_str());
    build_hdfs_test_config(&mut config_map);

    let cstore_config: CStoreConfig = CStoreConfigBuilder::default()
        .set_with_map(&config_map)
        .build();

    cstore_config.persistent.clone()
}

pub struct GraphDataBuilder {}

impl GraphDataBuilder {
    pub fn build_edge_graph_data(
        key: u32,
        graph_serde: &GraphSerde,
        label_dict: &LabelDict,
    ) -> GraphData {
        let edge = Edge::create_id_time_edge(
            key.to_be_bytes().to_vec(),
            (key + 1).to_be_bytes().to_vec(),
            0,
            EdgeDirection::In,
            (key + 2).to_be_bytes().to_vec(),
        );

        graph_serde.serialize_edge(&edge, label_dict)
    }

    pub fn build_vertex_graph_data(
        key: u32,
        graph_serde: &GraphSerde,
        label_dict: &LabelDict,
    ) -> GraphData {
        let vertex =
            Vertex::create_id_vertex(key.to_be_bytes().to_vec(), (key + 3).to_be_bytes().to_vec());

        graph_serde.serialize_vertex(&vertex, label_dict)
    }

    pub fn get_and_assert_data(engine: &Engine, key: u32) {
        let graph_data_vec_opt = engine.get(key, &create_empty_filter_pushdown());
        let graph_data_vec = graph_data_vec_opt
            .as_ref()
            .unwrap_or_else(|| panic!("key {} graph data not exist.", key));

        assert_eq!(graph_data_vec.len(), 2);

        for graph_data in graph_data_vec {
            if !is_vertex(graph_data.second_key.graph_info) {
                assert_eq!(
                    graph_data.second_key.target_id,
                    (key + 1).to_be_bytes().to_vec()
                );

                assert_eq!(graph_data.property, (key + 2).to_be_bytes().to_vec());
            } else {
                assert_eq!(graph_data.property, (key + 3).to_be_bytes().to_vec());
            }
        }
    }
}

pub struct TestStoreHelper {}

pub const INTEGRATION_TEST_LOCAL_ROOT: &str = "tmp/geaflow_cstore_local";
pub const INTEGRATION_TEST_REMOTE_ROOT: &str = "tmp/geaflow_cstore_remote";

const MEM_SEGMENT_SIZE_VECTOR: [usize; 3] = [16777216, 33554432, 67108864];
const FILE_SIZE_MULTIPLIER: [usize; 3] = [4, 8, 12];
const LEVEL0_FILE_NUM: [usize; 3] = [4, 8, 12];
const INDEX_GRANULARITY: [usize; 4] = [128, 512, 1024, 2048];
const LOCATION_TYPE: [&str; 2] = ["Local", "Remote"];

impl TestStoreHelper {
    pub fn randomly_construct_cstore_config(test_name: &str, shard_id: u32) -> CStoreConfig {
        let mut config_map = ConfigMap::default();

        let random_mem_segment_size = MEM_SEGMENT_SIZE_VECTOR
            .choose(&mut rand::thread_rng())
            .unwrap();
        let random_file_size_multiplier = FILE_SIZE_MULTIPLIER
            .choose(&mut rand::thread_rng())
            .unwrap();

        let random_level1_file_size =
            (random_mem_segment_size / 2 * random_file_size_multiplier).to_string();
        let random_mem_segment_size = random_mem_segment_size.to_string();
        let random_file_size_multiplier = random_file_size_multiplier.to_string();
        let random_level0_file_num = LEVEL0_FILE_NUM
            .choose(&mut rand::thread_rng())
            .unwrap()
            .to_string();
        let random_index_granularity = INDEX_GRANULARITY
            .choose(&mut rand::thread_rng())
            .unwrap()
            .to_string();
        let job_name = bind_job_name_with_ts("store_integration_test");
        let random_location =
            if test_name == "test_ldbc_sf10_source_1" || test_name == "test_ldbc_sf10_source_2" {
                "Local"
            } else {
                *LOCATION_TYPE.choose(&mut rand::thread_rng()).unwrap()
            };

        config_map.insert("local.root", INTEGRATION_TEST_LOCAL_ROOT);
        config_map.insert("persistent.root", INTEGRATION_TEST_REMOTE_ROOT);
        config_map.insert("store.store_name", test_name);
        config_map.insert("store.job_name", job_name.as_str());
        config_map.insert("store.mem_segment_size", &random_mem_segment_size);
        config_map.insert("store.file_size_multiplier", &random_file_size_multiplier);
        config_map.insert("store.level1_file_size", &random_level1_file_size);
        config_map.insert("store.level0_file_num", &random_level0_file_num);
        config_map.insert("index.index_granularity", &random_index_granularity);
        config_map.insert("manifest.max_manifest_num", "1");
        config_map.insert("store.location", random_location);

        let cstore_config = CStoreConfigBuilder::default()
            .set_with_map(&config_map)
            .set("store.shard_index", shard_id)
            .build();

        s_info!(
            shard_id,
            "random config info: mem_segment_size[{}] level1_file_size[{}], \
            file_size_multiplier[{}] level0_file_num[{}] index_granularity[{}] \
            random_location[{}]",
            random_mem_segment_size,
            random_level1_file_size,
            random_file_size_multiplier,
            random_level0_file_num,
            random_index_granularity,
            random_location,
        );

        cstore_config
    }

    pub fn insert_data(
        data: &GraphStruct,
        cstore: &mut CStore,
        mut edges_map: Option<&mut HashMap<u64, HashSet<edge::Edge>>>,
        mut vertex_map: Option<&mut HashMap<u64, Vertex>>,
        id_set: Option<&mut HashSet<u64>>,
    ) {
        let get_edge_direct_func = |edge: &LdbcEdge| match edge.direct {
            Direct::In => EdgeDirection::In,
            Direct::Out => EdgeDirection::Out,
        };
        match data {
            GraphStruct::Edge(edge) => {
                let src_id = edge.sid.encode_to_vec();
                let target_id = edge.tid.encode_to_vec();
                let ts = edge.ts;
                let label = edge.label.to_string();
                let property = edge.property.encode_to_vec();

                cstore.add_edge(edge::Edge::create_id_time_label_edge(
                    src_id.clone(),
                    target_id.clone(),
                    ts,
                    label.clone(),
                    get_edge_direct_func(edge),
                    property.clone(),
                ));

                if let Some(edges_map_ref) = &mut edges_map {
                    edges_map_ref.entry(edge.sid).or_default().replace(
                        edge::Edge::create_id_time_label_edge(
                            src_id,
                            target_id,
                            ts,
                            label,
                            get_edge_direct_func(edge),
                            property,
                        ),
                    );
                }

                if let Some(id_set_ref) = id_set {
                    id_set_ref.insert(edge.sid);
                }
            }
            GraphStruct::BothEdge(out_edge, in_edge) => {
                let src_id = out_edge.sid.encode_to_vec();
                let target_id = out_edge.tid.encode_to_vec();
                let ts = out_edge.ts;
                let label = out_edge.label.to_string();
                let property = out_edge.property.encode_to_vec();

                cstore.add_edge(edge::Edge::create_id_time_label_edge(
                    src_id.clone(),
                    target_id.clone(),
                    ts,
                    label.clone(),
                    EdgeDirection::Out,
                    property.clone(),
                ));

                if let Some(edges_map_ref) = &mut edges_map {
                    edges_map_ref.entry(out_edge.sid).or_default().replace(
                        edge::Edge::create_id_time_label_edge(
                            src_id,
                            target_id,
                            ts,
                            label,
                            get_edge_direct_func(out_edge),
                            property,
                        ),
                    );
                }

                let src_id = in_edge.sid.encode_to_vec();
                let target_id = in_edge.tid.encode_to_vec();
                let ts = in_edge.ts;
                let label = in_edge.label.to_string();
                let property = in_edge.property.encode_to_vec();

                cstore.add_edge(edge::Edge::create_id_time_label_edge(
                    src_id.clone(),
                    target_id.clone(),
                    ts,
                    label.clone(),
                    EdgeDirection::In,
                    property.clone(),
                ));

                if let Some(edges_map_ref) = &mut edges_map {
                    edges_map_ref.entry(in_edge.sid).or_default().replace(
                        edge::Edge::create_id_time_label_edge(
                            src_id,
                            target_id,
                            ts,
                            label,
                            get_edge_direct_func(in_edge),
                            property,
                        ),
                    );
                }

                if let Some(id_set_ref) = id_set {
                    id_set_ref.insert(in_edge.sid);
                }
            }
            GraphStruct::Vertex(vertex) => {
                let src_id = vertex.id.encode_to_vec();
                let ts = vertex.ts;
                let label = vertex.label.to_string();
                let property = vertex.property.encode_to_vec();

                cstore.add_vertex(Vertex::create_id_time_label_vertex(
                    src_id.clone(),
                    ts,
                    label.clone(),
                    property.clone(),
                ));

                if let Some(vertex_map_ref) = &mut vertex_map {
                    vertex_map_ref.insert(
                        vertex.id,
                        Vertex::create_id_time_label_vertex(
                            src_id.clone(),
                            ts,
                            label.clone(),
                            property.clone(),
                        ),
                    );
                }

                if let Some(id_set_ref) = id_set {
                    id_set_ref.insert(vertex.id);
                }
            }
            GraphStruct::None => {
                info!("none data")
            }
        }
    }
}
