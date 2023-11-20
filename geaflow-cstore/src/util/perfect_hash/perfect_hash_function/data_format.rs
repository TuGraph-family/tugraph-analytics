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

#![allow(unused_variables)]

use super::hash_function::{city_hash32, murmur_hash};

#[derive(Copy, Clone, Hash, Eq, PartialEq, Default)]
pub struct UInt64Struct {
    pub hash1: u32,
    pub hash2: u32,
}

impl UInt64Struct {
    fn new() -> UInt64Struct {
        UInt64Struct { hash1: 0, hash2: 0 }
    }
    fn with_hash(mut self, hash1: u32, hash2: u32) -> Self {
        self.hash1 = hash1;
        self.hash2 = hash2;
        self
    }
}

#[derive(Copy, Clone, Hash, Eq, PartialEq, Default)]
pub struct HashEntry {
    pub hash: UInt64Struct,
    pub bucket_index: u32,
}

impl HashEntry {
    pub fn new() -> HashEntry {
        HashEntry {
            hash: UInt64Struct::new(),
            bucket_index: 0,
        }
    }

    pub fn with_uint64struct(mut self, hash: UInt64Struct) -> Self {
        self.hash = hash;
        self
    }

    pub fn with_hash_value(mut self, hash1: u32, hash2: u32) -> Self {
        self.hash = UInt64Struct::new().with_hash(hash1, hash2);
        self
    }

    pub fn create_from_key(mut self, key: &[u8]) -> Self {
        self.hash.hash1 = murmur_hash(key);
        // replace djb2 for faster murmur hash
        // self.hash.hash1 = djb2_hash(&key);
        self.hash.hash2 = city_hash32(key);
        self
    }
}
