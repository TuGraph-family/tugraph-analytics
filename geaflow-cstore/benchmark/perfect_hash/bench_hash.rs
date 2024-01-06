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
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
    vec,
};

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use geaflow_cstore::perfect_hash::perfect_hash_function::hash_function::{
    city_hash32, djb2_hash, murmur_hash,
};
use rand::{thread_rng, Rng};

static KEY_SIZE: usize = 512;
static QUERY_COUNT: usize = 100000;

fn test_perfect(key_vec: &Vec<Vec<u8>>) {
    for i in 0..QUERY_COUNT {
        black_box(murmur_hash(&key_vec[i]));
        black_box(city_hash32(&key_vec[i]));
    }
}

fn test_std(key_vec: &Vec<Vec<u8>>) {
    for i in 0..QUERY_COUNT {
        let mut s = DefaultHasher::new();
        black_box(key_vec[i].hash(&mut s));
        s.finish();
    }
}

pub fn hash_benchmark(c: &mut Criterion) {
    let mut key_vec: Vec<Vec<u8>> = vec![];
    let mut key_buf: Vec<u8> = vec![0; KEY_SIZE];
    for _i in 0..QUERY_COUNT {
        for i in 0..KEY_SIZE as usize {
            key_buf[i] = thread_rng().gen::<u8>();
        }
        key_vec.push(key_buf.clone());
    }

    let mut group = c.benchmark_group("hash_bench");
    group.bench_function(format!("perfect"), |b| b.iter(|| test_perfect(&key_vec)));
    group.bench_function(format!("std"), |b| b.iter(|| test_std(&key_vec)));
    group.finish();
}

criterion_group!(benches, hash_benchmark);
criterion_main!(benches);
