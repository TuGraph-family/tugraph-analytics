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

use dashmap::DashMap;
use rustc_hash::FxHashSet;

use crate::ParentNode;

type MemIndexIter<'a> = dashmap::iter::Iter<'a, u32, ParentNode>;

pub struct IndexModifier<'a> {
    mem_index: &'a DashMap<u32, ParentNode>,
}

impl<'a> IndexModifier<'a> {
    pub fn new(mem_index: &'a DashMap<u32, ParentNode>) -> Self {
        IndexModifier { mem_index }
    }

    // Get Iterator of mem index, this operation would lock the mem index.
    pub fn iter(&self) -> MemIndexIter<'a> {
        let mem_index_iter: dashmap::iter::Iter<'a, u32, ParentNode> = self.mem_index.iter();
        mem_index_iter
    }

    pub fn insert(&self, key: u32, parent_node: ParentNode) {
        self.mem_index.insert(key, parent_node);
    }

    // thread safe, atomic update.
    pub fn gc_mem_index(&self, inner_key: u32, to_del: &FxHashSet<u32>) {
        self.mem_index.entry(inner_key).and_modify(|parent_node| {
            parent_node.gc(to_del);
        });

        // TODO, Temporarily unused, switch CsrMemIndex after optimization.
        // self.mem_index.apply(inner_key, |parent_node| {
        //     parent_node.update(to_add, to_del);
        // });
    }
}
