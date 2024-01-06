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
    collections::{hash_map::DefaultHasher, HashMap},
    hash::{Hash, Hasher},
    vec,
};

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use geaflow_cstore::perfect_hash::{
    perfect_hash_function::hash_function::{city_hash32, murmur_hash},
    perfect_hash_map::PerfectHashMap,
};
use rand::{thread_rng, Rng};

static KEY_SIZE: usize = 128;
static KEY_COUNT: usize = 1000000;
static QUERY_COUNT: usize = 100000;

fn test_perfect(query_vec: &Vec<Vec<u8>>, hash_map: &PerfectHashMap) {
    for i in query_vec {
        black_box(hash_map.get(i));
    }
}

fn test_std(query_vec: &Vec<Vec<u8>>, hash_map: &HashMap<Vec<u8>, Vec<u8>>) {
    for i in query_vec {
        black_box(hash_map.get(i));
    }
}

fn test_perfect_hash(query_vec: &Vec<Vec<u8>>) {
    for i in query_vec {
        black_box(murmur_hash(i));
        black_box(city_hash32(i));
    }
}

fn test_std_hash(query_vec: &Vec<Vec<u8>>) {
    for i in query_vec {
        let mut s = DefaultHasher::new();
        black_box(i.hash(&mut s));
        s.finish();
    }
}

fn test_iter(hash_map: &PerfectHashMap) {
    let mut x;
    for kv in hash_map {
        black_box(x = kv.1);
    }
}

pub fn map_benchmark(c: &mut Criterion) {
    let mut key_buf: Vec<u8> = vec![0; KEY_SIZE];
    let mut source: HashMap<Vec<u8>, Vec<u8>> = HashMap::default();
    let mut key_vec: Vec<Vec<u8>> = vec![];
    let mut query_vec: Vec<Vec<u8>> = vec![];
    // generate kv
    while source.len() < KEY_COUNT {
        for i in 0..KEY_SIZE {
            key_buf[i] = thread_rng().gen::<u8>();
        }
        source.insert(key_buf.clone(), vec![thread_rng().gen::<u8>(); 8]);
        key_vec.push(key_buf.clone());
    }
    let perfect = PerfectHashMap::new().build_from_map(&source).unwrap();

    for _i in 0..QUERY_COUNT {
        query_vec.push(key_vec[thread_rng().gen::<usize>() % KEY_COUNT].clone());
    }

    let mut group = c.benchmark_group("hashmap");
    group.bench_function(format!("perfect"), |b| {
        b.iter(|| test_perfect(&query_vec, &perfect))
    });
    group.bench_function(format!("std"), |b| b.iter(|| test_std(&query_vec, &source)));
    group.bench_function(format!("perfect hash time"), |b| {
        b.iter(|| test_perfect_hash(&query_vec))
    });
    group.bench_function(format!("std hash time"), |b| {
        b.iter(|| test_std_hash(&query_vec))
    });
    group.bench_function(format!("iter"), |b| b.iter(|| test_iter(&perfect)));
    group.finish();
}

criterion_group!(benches, map_benchmark);
criterion_main!(benches);
