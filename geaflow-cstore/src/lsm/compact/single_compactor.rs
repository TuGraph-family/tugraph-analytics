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
    fmt::{self, Formatter},
    sync::Arc,
    time::Instant,
};

use itertools::Itertools;

use super::compact_collect_strategy::CompactMeta;
use crate::{
    error::Error,
    error_and_panic,
    lsm::{
        compact::{
            compact_build_table_strategy::CompactBuildTableStrategy,
            compact_collect_strategy::CompactCollectStrategy,
            compact_update_lsm_strategy::CompactUpdateLsmStrategy,
        },
        compact_manager::CompactManagerInner,
        level_handler::TableInfo,
    },
    s_info, Result,
};

#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy)]
pub enum CompactStatus {
    NoNeedToCompact = 0,
    CompactOk,
}

#[derive(Debug, Default)]
pub struct CompactTimeCost {
    pub all_time: u128,
    pub build_new_tables_time: u128,
    pub update_lsm_time: u128,
    pub gc_time: u128,
}

impl fmt::Display for CompactTimeCost {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "all_time: {}ms, build_new_tables_time: {}ms, update_lsm_time: {}ms, gc_time: {}ms",
            self.all_time, self.build_new_tables_time, self.update_lsm_time, self.gc_time,
        )
    }
}

type CompactFileInfo = (u32, u32, u32, u32);

pub struct SingleCompactor {
    pub inner: Arc<CompactManagerInner>,
}

impl SingleCompactor {
    pub fn new(inner: Arc<CompactManagerInner>) -> Self {
        Self { inner }
    }

    // There are multiple compactors, one compactor binding to one thread.
    pub fn run_single_compactor(&self) -> CompactStatus {
        // pick need compact meta, including level info, file info and key range.
        let compact_meta_option = CompactCollectStrategy::pick_compact_meta(&self.inner);
        if compact_meta_option.is_none() {
            return CompactStatus::NoNeedToCompact;
        }

        let _compact_drop_read_guard = self.inner.compact_drop_lock.read().unwrap();

        let mut compact_time_cost = CompactTimeCost::default();
        let start_compact = Instant::now();
        let compact_meta = compact_meta_option.unwrap();
        let this_level_compaction_log_info =
            Self::get_compaction_log_info(&compact_meta.this_level_table_infos);
        let next_level_compaction_log_info =
            Self::get_compaction_log_info(&compact_meta.next_level_table_infos);

        s_info!(
            self.inner.store_config.shard_index,
            "start compacting, \
            compaction info[files format is (index_file_fid, data_file_fid, start_key, end_key)]: \
            low level {} compacted files {:?}, high level {} compacted files {:?}, compact type {}.",
            compact_meta.this_level,
            &this_level_compaction_log_info,
            compact_meta.next_level,
            &next_level_compaction_log_info,
            compact_meta.next_level_compact_type,
        );

        // Build need compact tables by compact meta, Compact and build new tables.
        let start_build_new_tables = Instant::now();
        let (new_table_infos, gc_meta) =
            CompactBuildTableStrategy::build_new_tables(&self.inner, &compact_meta);
        compact_time_cost.build_new_tables_time = start_build_new_tables.elapsed().as_millis();

        // Update level controller, include lsm tree and memory index.
        CompactUpdateLsmStrategy::update_memory_index_and_lsm_tree(
            &self.inner,
            &compact_meta,
            &new_table_infos,
            &gc_meta,
            &mut compact_time_cost,
        );
        compact_time_cost.all_time = start_compact.elapsed().as_millis();

        s_info!(
            self.inner.store_config.shard_index,
            "finish compacting, time cost info: {}, \
            compaction info[files format is (index_file_fid, data_file_fid, start_key, end_key)]: \
            low level {} compacted files {:?}, high level {} compacted files {:?}, compact type {}, \
            add new files {:?} to level {}.",
            compact_time_cost,
            compact_meta.this_level,
            &this_level_compaction_log_info,
            compact_meta.next_level,
            &next_level_compaction_log_info,
            compact_meta.next_level_compact_type,
            Self::get_compaction_log_info(&new_table_infos),
            compact_meta.next_level,
        );

        // Check compact, including whether the keys overlap.
        self.check_compact(&compact_meta).unwrap_or_else(|e| {
            error_and_panic!("{}", e);
        });

        CompactStatus::CompactOk
    }

    // fn check_compact
    fn check_compact(&self, compact_meta: &CompactMeta) -> Result<()> {
        let next_level = compact_meta.next_level;
        let level_read_guard = self
            .inner
            .level_controller
            .get_level_handler(next_level)
            .read()
            .unwrap();
        let mut table_info_vec = level_read_guard.get_table_info_vec().into_iter().peekable();
        let first_table_info = table_info_vec.next().unwrap();

        let mut compaction_info = (
            first_table_info.index_table_info.fid,
            first_table_info.data_table_info.fid,
            first_table_info.index_table_info.start,
            first_table_info.index_table_info.end,
        );

        if compaction_info.2 > compaction_info.3 {
            return Err(Error::CompactError(format!(
                "compact check fail, table info: {:?}",
                compaction_info
            )));
        }

        while table_info_vec.peek().is_some() {
            let table_info = table_info_vec.next().unwrap();

            let next_compaction_info = (
                table_info.index_table_info.fid,
                table_info.data_table_info.fid,
                table_info.index_table_info.start,
                table_info.index_table_info.end,
            );

            if next_compaction_info.2 <= compaction_info.3 {
                return Err(Error::CompactError(format!(
                    "compact check fail, table info: {:?} {:?}",
                    compaction_info, next_compaction_info
                )));
            }

            if next_compaction_info.2 > next_compaction_info.3 {
                return Err(Error::CompactError(format!(
                    "compact check fail, table info: {:?}",
                    next_compaction_info
                )));
            }
            compaction_info = next_compaction_info;
        }

        Ok(())
    }

    fn get_compaction_log_info(table_infos: &[TableInfo]) -> Vec<CompactFileInfo> {
        table_infos
            .iter()
            .map(|table_info| {
                (
                    table_info.index_table_info.fid,
                    table_info.data_table_info.fid,
                    table_info.index_table_info.start,
                    table_info.index_table_info.end,
                )
            })
            .collect_vec()
    }
}
