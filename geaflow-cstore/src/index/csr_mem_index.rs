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

#![allow(dead_code)]

use std::sync::{
    atomic::{AtomicU32, Ordering},
    Arc, RwLock,
};

use super::parent_node::ParentNode;
use crate::{config::IndexConfig, convert_usize_to_u32};

// TODO, opt rwlock of csr mem index.
type MemIndexValueVec = Arc<RwLock<Vec<Arc<RwLock<Vec<ParentNode>>>>>>;

pub struct CsrMemIndex {
    vindex: MemIndexValueVec,

    index_num: AtomicU32,

    index_granularity: u32,

    index_vector_len: u32,

    index_vector_len_mod_mask: u32,

    index_vector_len_bit_shift: u32,
}

impl CsrMemIndex {
    pub fn new(index_config: &IndexConfig) -> Self {
        let index_vector_len_bit_shift = index_config.index_vector_len_bit_shift;

        let csr_mem_index = CsrMemIndex {
            vindex: MemIndexValueVec::default(),
            index_num: AtomicU32::new(0),
            index_granularity: index_config.index_granularity,
            index_vector_len: 1u32 << index_vector_len_bit_shift,
            index_vector_len_mod_mask: (1u32 << index_vector_len_bit_shift) - 1,
            index_vector_len_bit_shift,
        };

        csr_mem_index
            .vindex
            .write()
            .unwrap()
            .push(Arc::new(RwLock::new(
                std::iter::repeat_with(ParentNode::new)
                    .take(csr_mem_index.index_vector_len as usize)
                    .collect(),
            )));

        csr_mem_index
    }

    pub fn put(&self, inner_key: u32, val: ParentNode) {
        let pos = self.check_and_get_pos(inner_key) as usize;
        let idx = (inner_key & self.index_vector_len_mod_mask) as usize;

        let read_guard = self.vindex.read().unwrap();
        let mut write_guard = read_guard.get(pos).unwrap().write().unwrap();

        if write_guard.get(idx).unwrap().is_empty() {
            self.index_num.fetch_add(1, Ordering::SeqCst);
        }

        write_guard[idx] = val;
    }

    pub fn apply(&self, inner_key: u32, f: impl FnOnce(&mut ParentNode)) {
        let pos = self.check_and_get_pos(inner_key) as usize;
        let idx = (inner_key & self.index_vector_len_mod_mask) as usize;

        let read_guard = self.vindex.read().unwrap();

        let mut write_guard = read_guard.get(pos).unwrap().write().unwrap();

        if write_guard.get(idx).unwrap().is_empty() {
            self.index_num.fetch_add(1, Ordering::SeqCst);
        }

        f(write_guard.get_mut(idx).unwrap());
    }

    pub fn check_and_get_pos(&self, size: u32) -> u32 {
        let pos = size >> self.index_vector_len_bit_shift;
        let len: usize = (pos + 1) as usize;

        if self.vindex.read().unwrap().len() >= len {
            return pos;
        }

        let mut write_guard = self.vindex.write().unwrap();

        if write_guard.len() < len {
            let init_size = write_guard.len();
            for _i in init_size..len {
                write_guard.push(Arc::new(RwLock::new(
                    std::iter::repeat_with(ParentNode::default)
                        .take(self.index_vector_len as usize)
                        .collect(),
                )));
            }
        }

        pos
    }

    pub fn remove(&self, inner_key: u32) -> Option<ParentNode> {
        if inner_key
            < convert_usize_to_u32!(self.vindex.read().unwrap().len()) * self.index_vector_len
        {
            let pos = (inner_key >> self.index_vector_len_bit_shift) as usize;
            let idx = (inner_key & self.index_vector_len_mod_mask) as usize;
            let read_guard = self.vindex.read().unwrap();
            let mut write_guard = read_guard.get(pos).unwrap().write().unwrap();

            if !write_guard.get(idx).unwrap().is_empty() {
                self.index_num.fetch_sub(1, Ordering::SeqCst);
                let parent_node = write_guard.get(idx).unwrap().clone();

                write_guard.get_mut(idx).unwrap().close();

                return Some(parent_node);
            }
        }
        None
    }

    pub fn size(&self) -> u32 {
        self.index_num.load(Ordering::SeqCst)
    }

    pub fn close(&self) {
        let read_guard = &mut self.vindex.read().unwrap();

        for i in 0..read_guard.len() {
            let mut write_guard = read_guard.get(i).unwrap().write().unwrap();

            for j in 0..write_guard.len() {
                write_guard.get_mut(j).unwrap().close();
            }
        }
    }

    pub fn get(&self, inner_key: &u32) -> Option<ParentNode> {
        let pos = inner_key >> self.index_vector_len_bit_shift;

        if pos > self.size() - 1 {
            return None;
        }

        let idx = (inner_key & self.index_vector_len_mod_mask) as usize;

        let read_guard = self.vindex.read().unwrap();
        let inner_read_guard = read_guard.get(pos as usize).unwrap().read().unwrap();

        if !inner_read_guard.get(idx).unwrap().is_empty() {
            return Some(inner_read_guard.get(idx).unwrap().clone());
        }

        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        config::CStoreConfig,
        index::parent_node::IndexMeta,
        log_util::{self, info, LogLevel, LogType},
    };

    const INDEX_META_NUM: usize = 10;
    const VEC_PARENT_NODE_LAYERS: u32 = 16;

    #[ignore = "Timing task execute it"]
    #[test]
    fn test_csr_mem_index() {
        log_util::try_init(LogType::ConsoleAndFile, LogLevel::Debug, 0);

        let cstore_config = CStoreConfig::default();
        let index_config = &cstore_config.index;
        let csr_mem_index = Arc::new(CsrMemIndex::new(index_config));

        let index_vector_len = 2u32.pow(index_config.index_vector_len_bit_shift);

        // Multi-thread job handle vec.
        let mut handles = Vec::new();

        for i in 0..VEC_PARENT_NODE_LAYERS {
            let csr_mem_index = Arc::clone(&csr_mem_index);
            let handle = std::thread::spawn(move || {
                for j in 0..index_vector_len {
                    for k in 0..INDEX_META_NUM {
                        let index_meta = IndexMeta {
                            fid: i * index_vector_len + j + 1,
                            offset: k as u64 + 2,
                        };
                        csr_mem_index.apply(i * index_vector_len + j, |parent_node| {
                            parent_node.append(index_meta);
                        });
                    }
                }

                for j in 0..index_vector_len {
                    let mut parent_node = ParentNode::new();
                    for k in 0..INDEX_META_NUM {
                        let index_meta = IndexMeta {
                            fid: i * index_vector_len + j + 1,
                            offset: k as u64 + 2,
                        };
                        parent_node.append(index_meta);
                    }

                    let value = csr_mem_index.get(&(i * index_vector_len + j)).unwrap();
                    assert_eq!(value, parent_node);
                }

                if i == 1 {
                    csr_mem_index.get(&index_vector_len).unwrap();
                    csr_mem_index.remove(index_vector_len);
                    let value = csr_mem_index.get(&index_vector_len);
                    assert_eq!(value, None);
                }
                info!("Thread {} executing CsrMemIndex Test over.", i);
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
        }
    }
}
