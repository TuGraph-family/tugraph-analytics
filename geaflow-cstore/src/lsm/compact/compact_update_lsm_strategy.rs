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

use std::time::Instant;

use rustc_hash::{FxHashMap, FxHashSet};

use super::{
    super::compact_manager::CompactManagerInner, compact_collect_strategy::CompactMeta,
    single_compactor::CompactTimeCost,
};
use crate::{log_util::trace, lsm::level_handler::TableInfo};

pub struct CompactUpdateLsmStrategy {}

impl CompactUpdateLsmStrategy {
    // Update level controller, include lsm tree and memory index.
    pub fn update_memory_index_and_lsm_tree(
        inner: &CompactManagerInner,
        compact_meta: &CompactMeta,
        new_table_infos: &[TableInfo],
        gc_meta: &FxHashMap<u32, FxHashSet<u32>>,
        compact_time_cost: &mut CompactTimeCost,
    ) {
        let _lock_guard = inner.level_controller.archive_read_guard();

        let (this_level_to_del_index_fids, next_level_to_del_index_fids, to_del_fids) =
            Self::collect_to_delete_fids(compact_meta);

        let start_update_lsm = Instant::now();
        // update new files to this level.
        inner.level_controller.update_lsm(
            new_table_infos,
            &next_level_to_del_index_fids,
            compact_meta.next_level,
        );
        compact_time_cost.update_lsm_time = start_update_lsm.elapsed().as_millis();

        // remove stale fid from index.
        let start_gc = Instant::now();
        Self::gc_index(inner, gc_meta);
        compact_time_cost.gc_time = start_gc.elapsed().as_millis();

        // Record the fids of index and data files which are prepared to drop.
        for (index_table_fid, data_table_fid) in to_del_fids.iter() {
            inner.fids_to_drop.insert(*data_table_fid, *index_table_fid);

            // TODO Optimise iterator to remove using cache in compactor.
            inner
                .engine_context
                .cache_handler
                .file_cache
                .remove(index_table_fid);
            inner
                .engine_context
                .cache_handler
                .table_meta_cache
                .remove(index_table_fid);
        }

        // Delete the compacted files in this level.
        inner
            .level_controller
            .delete_lsm(&this_level_to_del_index_fids, compact_meta.this_level);
    }

    fn collect_to_delete_fids(compact_meta: &CompactMeta) -> (Vec<u32>, Vec<u32>, Vec<(u32, u32)>) {
        // Collect the fids of index files to delete from lsm tree in this level.
        let mut this_level_to_del_index_fids = vec![];
        // Collect the fids of index files to delete from lsm tree in next level.
        let mut next_level_to_del_index_fids = vec![];

        // Collect the fids of index and data files to drop from disk in this level and
        // next level.
        let mut to_del_fids = vec![];

        compact_meta
            .this_level_table_infos
            .iter()
            .for_each(|table_info| {
                this_level_to_del_index_fids.push(table_info.index_table_info.fid);
                to_del_fids.push((
                    table_info.index_table_info.fid,
                    table_info.data_table_info.fid,
                ));
            });

        compact_meta
            .next_level_table_infos
            .iter()
            .for_each(|table_info| {
                next_level_to_del_index_fids.push(table_info.index_table_info.fid);
                to_del_fids.push((
                    table_info.index_table_info.fid,
                    table_info.data_table_info.fid,
                ));
            });

        (
            this_level_to_del_index_fids,
            next_level_to_del_index_fids,
            to_del_fids,
        )
    }

    fn gc_index(inner: &CompactManagerInner, gc_meta: &FxHashMap<u32, FxHashSet<u32>>) {
        let index_modifier = inner.csr_index.create_index_modifier();
        trace!(
            "gc index del fids {:?}",
            gc_meta
                .iter()
                .flat_map(|item| item.1.iter())
                .copied()
                .collect::<FxHashSet<u32>>()
        );
        for entry in gc_meta.iter() {
            // update index.
            index_modifier.gc_mem_index(*entry.0, entry.1);
        }
    }
}
