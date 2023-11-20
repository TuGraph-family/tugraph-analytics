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
    cmp::Ordering,
    collections::{BinaryHeap, VecDeque},
    iter::Peekable,
    rc::Rc,
    vec::IntoIter,
};

use crate::{
    api::{
        filter::StoreFilterPushdown,
        graph::graph_comparator::{cmp_second_key, GraphSortField},
    },
    config::TableConfig,
    error_and_panic,
    index::index_searcher::{get_block, SingleIndex},
    serialize_util::{deserialize_index_with_filter_pushdown, SerializeType},
    EngineContext, GraphData, ReadableTable, SequentialReadIter, TableContext, TableType,
};

/// This iterator return sorted and merged graph data with given is file
/// iterators. Details as follows:
/// 1. Compare multiple head block of is file iterators and get the greater key.
/// 2. Deserialize index.
/// 3. Search property and get the graph data.
/// 4. Sort all graph data.
/// 5. return sorted and merged graph data.
pub struct ScanDataMergeIterator<'a> {
    heap: BinaryHeap<ComparableScanDataIterator<'a>>,

    engine_context: &'a EngineContext,
}

impl<'a> ScanDataMergeIterator<'a> {
    pub fn new(
        multi_sorted_table_ids: Vec<Vec<u32>>,
        engine_context: &'a EngineContext,
        table_config: &'a TableConfig,
        filter_pushdown: Rc<StoreFilterPushdown>,
        enable_cache: bool,
    ) -> ScanDataMergeIterator<'a> {
        let mut heap = BinaryHeap::new();
        for sorted_table_iter in multi_sorted_table_ids {
            if sorted_table_iter.is_empty() {
                continue;
            }
            let index_iterator = IndexIterator::new(
                engine_context,
                table_config,
                sorted_table_iter.into_iter(),
                enable_cache,
            )
            .peekable();

            let mut graph_data_iterator = ScanDataIterator::new(
                index_iterator,
                Rc::clone(&filter_pushdown),
                engine_context,
                table_config,
                enable_cache,
            )
            .peekable();

            if graph_data_iterator.peek().is_some() {
                let head_key_to_graph_data = graph_data_iterator.next().unwrap();

                heap.push(ComparableScanDataIterator::new(
                    graph_data_iterator,
                    head_key_to_graph_data,
                    engine_context.sort_field.as_slice(),
                ));
            }
        }

        ScanDataMergeIterator {
            heap,
            engine_context,
        }
    }
}

impl<'a> Iterator for ScanDataMergeIterator<'a> {
    type Item = MergedGraphData;

    fn next(&mut self) -> Option<Self::Item> {
        if self.heap.is_empty() {
            return None;
        }

        let comparable_graph_data_iterator = self.heap.pop().unwrap();
        let mut graph_data_iterator = comparable_graph_data_iterator.graph_data_iterator;
        if graph_data_iterator.peek().is_some() {
            let head_key_to_graph_data = graph_data_iterator.next().unwrap();

            self.heap.push(ComparableScanDataIterator::new(
                graph_data_iterator,
                head_key_to_graph_data,
                self.engine_context.sort_field.as_slice(),
            ));
        }
        Some(comparable_graph_data_iterator.head_key_to_graph_data)
    }
}

#[derive(Debug)]
pub struct MergedGraphData {
    pub key: u32,
    pub fid: u32,
    pub graph_data: GraphData,
}

struct ComparableScanDataIterator<'a> {
    graph_data_iterator: Peekable<ScanDataIterator<'a>>,
    head_key_to_graph_data: MergedGraphData,
    sort_field: &'a [GraphSortField],
}

impl<'a> ComparableScanDataIterator<'a> {
    pub fn new(
        graph_data_iterator: Peekable<ScanDataIterator<'a>>,
        head_key_to_graph_data: MergedGraphData,
        sort_field: &'a [GraphSortField],
    ) -> Self {
        Self {
            graph_data_iterator,
            head_key_to_graph_data,
            sort_field,
        }
    }
}

impl<'a> Eq for ComparableScanDataIterator<'a> {}

impl<'a> PartialEq<Self> for ComparableScanDataIterator<'a> {
    fn eq(&self, other: &Self) -> bool {
        let ordering = self
            .head_key_to_graph_data
            .key
            .cmp(&other.head_key_to_graph_data.key);
        if ordering.is_eq() {
            cmp_second_key(
                &self.head_key_to_graph_data.graph_data.second_key,
                &other.head_key_to_graph_data.graph_data.second_key,
                self.sort_field,
            )
            .is_eq()
        } else {
            false
        }
    }
}

impl<'a> PartialOrd<Self> for ComparableScanDataIterator<'a> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<'a> Ord for ComparableScanDataIterator<'a> {
    fn cmp(&self, other: &Self) -> Ordering {
        let ordering = other
            .head_key_to_graph_data
            .key
            .cmp(&self.head_key_to_graph_data.key);
        if ordering.is_eq() {
            cmp_second_key(
                &other.head_key_to_graph_data.graph_data.second_key,
                &self.head_key_to_graph_data.graph_data.second_key,
                self.sort_field,
            )
        } else {
            ordering
        }
    }
}

struct ScanDataIterator<'a> {
    index_iterator: Peekable<IndexIterator<'a>>,
    filter_pushdown: Rc<StoreFilterPushdown>,
    engine_context: &'a EngineContext,
    table_config: &'a TableConfig,
    head_key: u32,
    head_fid: u32,
    head_graph_data_iter: Peekable<IntoIter<GraphData>>,
    enable_cache: bool,
}

impl<'a> ScanDataIterator<'a> {
    pub fn new(
        mut index_iterator: Peekable<IndexIterator<'a>>,
        filter_pushdown: Rc<StoreFilterPushdown>,
        engine_context: &'a EngineContext,
        table_config: &'a TableConfig,
        enable_cache: bool,
    ) -> Self {
        if index_iterator.peek().is_some() {
            let single_index = index_iterator.next().unwrap();

            let head_graph_data_index = deserialize_index_with_filter_pushdown(
                SerializeType::ISerde,
                single_index.second_keys.as_slice(),
                &filter_pushdown.filter_handler,
                &engine_context.ttl_dict,
            );

            let head_graph_data_vec = engine_context.table_searcher.search_and_attach_property(
                table_config,
                engine_context,
                head_graph_data_index,
                enable_cache,
                &filter_pushdown.filter_context,
            );

            return Self {
                index_iterator,
                filter_pushdown,
                engine_context,
                table_config,
                head_key: single_index.key,
                head_fid: single_index.fid,
                head_graph_data_iter: head_graph_data_vec.into_iter().peekable(),
                enable_cache,
            };
        }

        Self {
            index_iterator,
            filter_pushdown,
            engine_context,
            table_config,
            head_key: 0,
            head_fid: 0,
            head_graph_data_iter: vec![].into_iter().peekable(),
            enable_cache,
        }
    }
}

impl<'a> Iterator for ScanDataIterator<'a> {
    type Item = MergedGraphData;

    fn next(&mut self) -> Option<Self::Item> {
        while self.head_graph_data_iter.peek().is_none() && self.index_iterator.peek().is_some() {
            let single_index = self.index_iterator.next().unwrap();

            let head_graph_data_index = deserialize_index_with_filter_pushdown(
                SerializeType::ISerde,
                single_index.second_keys.as_slice(),
                &self.filter_pushdown.filter_handler,
                &self.engine_context.ttl_dict,
            );

            let head_graph_data_vec = self
                .engine_context
                .table_searcher
                .search_and_attach_property(
                    self.table_config,
                    self.engine_context,
                    head_graph_data_index,
                    self.enable_cache,
                    &self.filter_pushdown.filter_context,
                );

            self.head_key = single_index.key;
            self.head_fid = single_index.fid;
            self.head_graph_data_iter = head_graph_data_vec.into_iter().peekable();
        }

        if self.head_graph_data_iter.peek().is_some() {
            return Some(MergedGraphData {
                key: self.head_key,
                fid: self.head_fid,
                graph_data: self.head_graph_data_iter.next().unwrap(),
            });
        }

        None
    }
}

struct IndexIterator<'a> {
    engine_context: &'a EngineContext,

    table_config: &'a TableConfig,

    cur_fid: u32,

    single_level_table_iterator: IntoIter<u32>,

    table_iterator: Peekable<SequentialReadIter<'a>>,

    cur_block_iterator: Peekable<IndexBlockIterator>,

    enable_cache: bool,
}

impl<'a> IndexIterator<'a> {
    pub fn new(
        engine_context: &'a EngineContext,
        table_config: &'a TableConfig,
        mut sorted_table_iterator: IntoIter<u32>,
        enable_cache: bool,
    ) -> IndexIterator<'a> {
        let fid_opt = sorted_table_iterator.next();
        assert!(fid_opt.is_some());
        let fid = fid_opt.unwrap();
        let cur_table = ReadableTable::new(
            table_config,
            TableContext::new(table_config, fid, &engine_context.file_operator)
                .with_table_type(TableType::Is)
                .with_enable_cache(enable_cache),
            &engine_context.cache_handler,
        )
        .unwrap_or_else(|e| {
            error_and_panic!("error occurred in new index iterator: {}", e);
        });

        let table_iterator = SequentialReadIter::new(cur_table);

        let mut peekable_table_iterator = table_iterator.peekable();
        assert!(peekable_table_iterator.peek().is_some());
        let serialized_block = peekable_table_iterator.next().unwrap();
        IndexIterator {
            engine_context,
            table_config,
            cur_fid: fid,
            single_level_table_iterator: sorted_table_iterator,
            table_iterator: peekable_table_iterator,
            cur_block_iterator: IndexBlockIterator::new(fid, serialized_block).peekable(),
            enable_cache,
        }
    }

    fn get_next_table(&mut self) -> bool {
        // check current table iterator is empty, then use next table.
        // table should be closed in table_iterator.
        assert!(self.table_iterator.next().is_none());

        if let Some(fid) = self.single_level_table_iterator.next() {
            let cur_table = ReadableTable::new(
                self.table_config,
                TableContext::new(self.table_config, fid, &self.engine_context.file_operator)
                    .with_table_type(TableType::Is)
                    .with_enable_cache(self.enable_cache),
                &self.engine_context.cache_handler,
            )
            .unwrap_or_else(|e| {
                error_and_panic!("{}", e);
            });

            let table_iterator = SequentialReadIter::new(cur_table);
            self.table_iterator = table_iterator.peekable();
            self.cur_fid = fid;

            return true;
        }
        false
    }

    // return true if has next block.
    fn get_next_block(&mut self) -> bool {
        if self.table_iterator.peek().is_some() {
            let serialized_block = self.table_iterator.next().unwrap();
            self.cur_block_iterator =
                IndexBlockIterator::new(self.cur_fid, serialized_block).peekable();
            true
        } else {
            false
        }
    }

    fn get_next_key_value(&mut self) -> Option<SingleIndex> {
        if self.cur_block_iterator.peek().is_some() || self.get_next_block() {
            self.cur_block_iterator.next()
        } else if self.get_next_table() {
            assert!(self.get_next_block());
            self.cur_block_iterator.next()
        } else {
            None
        }
    }
}

impl<'a> Iterator for IndexIterator<'a> {
    type Item = SingleIndex;

    fn next(&mut self) -> Option<Self::Item> {
        self.get_next_key_value()
    }
}

struct IndexBlockIterator {
    deserialized_block: VecDeque<SingleIndex>,
}

impl IndexBlockIterator {
    pub fn new(fid: u32, serialized_block: Vec<Vec<u8>>) -> IndexBlockIterator {
        if serialized_block.is_empty() {
            IndexBlockIterator {
                deserialized_block: VecDeque::new(),
            }
        } else {
            IndexBlockIterator {
                deserialized_block: get_block(fid, serialized_block[0].as_slice()),
            }
        }
    }
}

impl Iterator for IndexBlockIterator {
    type Item = SingleIndex;

    fn next(&mut self) -> Option<Self::Item> {
        self.deserialized_block.pop_front()
    }
}
