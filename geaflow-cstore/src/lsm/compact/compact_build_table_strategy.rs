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

use std::{rc::Rc, time::Instant};

use rustc_hash::{FxHashMap, FxHashSet};

use super::compact_collect_strategy::CompactMeta;
use crate::{
    api::{
        filter::create_empty_filter_pushdown,
        graph::{graph_comparator::cmp_second_key, graph_info_util::is_vertex},
    },
    error_and_panic,
    index::index_builder::IndexBuilder,
    iterator::scan_data_iterator::ScanDataMergeIterator,
    lsm::{
        compact_manager::CompactManagerInner, level_handler::TableInfo, lsm_freezer::LsmFreezer,
    },
    s_trace, GraphData, SecondKey, TableType, WritableTable,
};

pub struct CompactBuildTableStrategy {}

impl CompactBuildTableStrategy {
    pub fn build_new_tables(
        inner: &CompactManagerInner,
        compact_meta: &CompactMeta,
    ) -> (Vec<TableInfo>, FxHashMap<u32, FxHashSet<u32>>) {
        let multi_sorted_tables = Self::build_need_compact_tables(compact_meta);

        let mut new_table_infos = vec![];
        let mut gc_meta: FxHashMap<u32, FxHashSet<u32>> = FxHashMap::default();

        let mut graph_data_merge_iterator = ScanDataMergeIterator::new(
            multi_sorted_tables,
            inner.engine_context.as_ref(),
            &inner.table_config,
            Rc::new(create_empty_filter_pushdown()),
            false,
        )
        .peekable();

        let mut has_vertex = false;
        while graph_data_merge_iterator.peek().is_some() {
            let mut new_vs_table: WritableTable;
            let mut update_index_builder: IndexBuilder;

            // create new data table.
            new_vs_table = inner
                .level_controller
                .create_new_table(&inner.table_config, TableType::Vs)
                .unwrap_or_else(|e| {
                    error_and_panic!(
                        "error occurred in building property table in compacting: {}",
                        e
                    );
                });

            // create index builder.
            update_index_builder = inner
                .csr_index
                .create_index_builder(
                    inner.level_controller.as_ref(),
                    &inner.table_config,
                    compact_meta.next_level,
                )
                .unwrap_or_else(|e| {
                    error_and_panic!(
                        "error occurred in building index table in compacting: {}",
                        e
                    );
                });

            let mut graph_data_vec_per_key: Vec<GraphData> = vec![];

            let start_iter_merge_iter = Instant::now();
            while let Some(merged_graph_data) = graph_data_merge_iterator.next() {
                let mut keep = true;

                // should be filter by ttl.
                if merged_graph_data.graph_data.second_key.is_empty() {
                    keep = false;
                }

                if keep {
                    // keep first vertex and delete duplicated edge.
                    if is_vertex(merged_graph_data.graph_data.second_key.graph_info) {
                        if has_vertex {
                            keep = false;
                        } else {
                            has_vertex = true;
                        }
                    } else {
                        let pre_graph_data_opt = graph_data_vec_per_key.last();
                        if let Some(pre_graph_data) = pre_graph_data_opt {
                            // check graph data order.
                            if is_vertex(pre_graph_data.second_key.graph_info) {
                                Self::check_graph_order(
                                    inner,
                                    &merged_graph_data.graph_data.second_key,
                                    &pre_graph_data.second_key,
                                );
                            }

                            // delete duplicated edge.
                            if merged_graph_data.graph_data.second_key.graph_info
                                == pre_graph_data.second_key.graph_info
                                && merged_graph_data
                                    .graph_data
                                    .second_key
                                    .target_id
                                    .eq(&pre_graph_data.second_key.target_id)
                            {
                                keep = false;
                            }
                        }
                    }
                }

                if keep {
                    graph_data_vec_per_key.push(merged_graph_data.graph_data);
                }

                gc_meta
                    .entry(merged_graph_data.key / inner.csr_index.index_granularity)
                    .or_default()
                    .insert(merged_graph_data.fid);

                let peek = graph_data_merge_iterator.peek();
                if peek.is_none() || peek.unwrap().key != merged_graph_data.key {
                    LsmFreezer::freeze_sorted_data(
                        &inner.store_config,
                        &mut update_index_builder,
                        &merged_graph_data.key,
                        &graph_data_vec_per_key,
                        &mut new_vs_table,
                    )
                    .unwrap_or_else(|e| {
                        error_and_panic!(
                            "error occurred in freezing property table in compacting: {}",
                            e
                        );
                    });
                    graph_data_vec_per_key.clear();
                    has_vertex = false;

                    // if reach index file capacity threshold, need create new data table and index
                    // builder.
                    if update_index_builder.reach_capacity_threshold() {
                        break;
                    }
                }
            }
            s_trace!(
                inner.store_config.base.shard_index,
                "iter merge iter cost {}ms",
                start_iter_merge_iter.elapsed().as_millis(),
            );

            LsmFreezer::flush_sorted_data(&mut new_vs_table).unwrap_or_else(|e| {
                error_and_panic!(
                    "error occurred in flushing property table in compacting: {}",
                    e
                );
            });

            new_table_infos.push(
                update_index_builder
                    .flush(new_vs_table.get_fid(), new_vs_table.get_table_offset())
                    .unwrap_or_else(|e| {
                        error_and_panic!(
                            "error occurred in flushing index table in compacting: {}",
                            e
                        );
                    }),
            );
        }

        (new_table_infos, gc_meta)
    }

    fn build_need_compact_tables(compact_meta: &CompactMeta) -> Vec<Vec<u32>> {
        // collect need compact table fid.
        let mut multi_sorted_table_ids = vec![];
        if compact_meta.this_level == 0 {
            for table_info in compact_meta.this_level_table_infos.iter() {
                multi_sorted_table_ids.push(vec![table_info.index_table_info.fid]);
            }
        } else {
            let mut fid_vec = vec![];
            for table_info in compact_meta.this_level_table_infos.iter() {
                fid_vec.push(table_info.index_table_info.fid);
            }
            multi_sorted_table_ids.push(fid_vec);
        }

        let mut fid_vec = vec![];
        for table_info in compact_meta.next_level_table_infos.iter() {
            fid_vec.push(table_info.index_table_info.fid);
        }
        multi_sorted_table_ids.push(fid_vec);

        multi_sorted_table_ids
    }

    fn check_graph_order(
        inner: &CompactManagerInner,
        second_key_1: &SecondKey,
        second_key_2: &SecondKey,
    ) {
        let ordering = cmp_second_key(second_key_1, second_key_2, &inner.engine_context.sort_field);

        assert!(ordering.is_gt());
    }
}
