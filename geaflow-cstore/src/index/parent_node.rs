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

use rustc_hash::FxHashSet;

#[derive(Hash, Eq, PartialEq, Clone, Debug)]
pub struct IndexMeta {
    pub fid: u32,

    pub offset: u64,
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct ParentNode {
    pub index_meta_vec: Vec<IndexMeta>,
}

impl ParentNode {
    pub fn new() -> Self {
        ParentNode {
            index_meta_vec: vec![],
        }
    }

    pub fn gc(&mut self, to_del: &FxHashSet<u32>) {
        if !to_del.is_empty() {
            self.index_meta_vec
                .retain(|index_meta| !to_del.contains(&index_meta.fid));
        }
    }

    pub fn append(&mut self, index_meta: IndexMeta) {
        self.index_meta_vec.push(index_meta);
    }

    pub fn append_list(&mut self, to_add: &[IndexMeta]) {
        self.index_meta_vec.extend(to_add.iter().cloned());
    }

    pub fn get(&self) -> &[IndexMeta] {
        self.index_meta_vec.as_slice()
    }

    pub fn is_empty(&self) -> bool {
        self.index_meta_vec.is_empty()
    }

    pub fn close(&mut self) {
        self.index_meta_vec.clear();
    }
}
