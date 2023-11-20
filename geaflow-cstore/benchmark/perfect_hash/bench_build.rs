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
    collections::HashMap,
    hash::{Hash, Hasher},
    vec,
};

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use geaflow_cstore::perfect_hash::perfect_hash_map::PerfectHashMap;
use rand::{thread_rng, Rng};

static KEY_SIZE: usize = 32;
static KEY_COUNT: usize = 100000;

fn test_perfect_vec(source_vec: &Vec<(Vec<u8>, Vec<u8>)>) {
    black_box(PerfectHashMap::new().build_from_vec(&source_vec).unwrap());
}

fn test_perfect_map(source: &HashMap<Vec<u8>, Vec<u8>>) {
    black_box(PerfectHashMap::new().build_from_map(&source).unwrap());
}

fn test_perfect_deserialize(binary: Vec<u8>) {
    black_box(PerfectHashMap::new().deserialize(binary));
}

fn test_std(source_vec: &Vec<(Vec<u8>, Vec<u8>)>) {
    let mut hash_map: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();
    for i in source_vec {
        black_box(hash_map.insert(i.0.clone(), i.1.clone()));
    }
}

pub fn map_benchmark(c: &mut Criterion) {
    let mut key_buf: Vec<u8> = vec![0; KEY_SIZE];
    let mut source: HashMap<Vec<u8>, Vec<u8>> = HashMap::default();
    let mut source_vec: Vec<(Vec<u8>, Vec<u8>)> = vec![];
    let mut key_vec: Vec<Vec<u8>> = vec![];
    // generate kv
    while source.len() < KEY_COUNT {
        for i in 0..KEY_SIZE {
            key_buf[i] = thread_rng().gen::<u8>();
        }
        let value = vec![thread_rng().gen::<u8>(); 8];
        source.insert(key_buf.clone(), value.clone());
        source_vec.push((key_buf.clone(), value.clone()));
        key_vec.push(key_buf.clone());
    }

    let mut group = c.benchmark_group("hashmap");
    let binary = PerfectHashMap::new()
        .build_from_vec(&source_vec)
        .unwrap()
        .serialize();
    group.bench_function(format!("perfect map"), |b| {
        b.iter(|| test_perfect_map(&source))
    });
    group.bench_function(format!("perfect vec"), |b| {
        b.iter(|| test_perfect_vec(&source_vec))
    });
    group.bench_function(format!("perfect deserialize"), |b| {
        b.iter(|| test_perfect_deserialize(binary.clone()))
    });
    group.bench_function(format!("std"), |b| b.iter(|| test_std(&source_vec)));
    group.finish();
}

criterion_group!(benches, map_benchmark);
criterion_main!(benches);
