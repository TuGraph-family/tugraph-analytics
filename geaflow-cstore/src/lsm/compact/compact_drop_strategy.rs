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

use std::{path::Path, sync::atomic::Ordering};

use rustc_hash::FxHashSet;

use crate::{
    error_and_panic,
    lsm::compact_manager::CompactManagerInner,
    s_info,
    table::get_table_type,
    util::{
        drop_util::{
            drop_files_multi_thread, get_registered_index_and_data_files_info, DropFileType,
        },
        number_util::get_integer_from_str_prefix,
        thread_util::RayonThreadPool,
    },
    DATA_AND_INDEX_FILE_NAME_DELIMITER,
};

pub struct CompactDropStrategy {}

impl CompactDropStrategy {
    pub fn drop_expired_index_data_files(
        compact_drop_thread_pool: &RayonThreadPool,
        inner: &CompactManagerInner,
    ) {
        let mut current = inner.compact_trigger_counts_for_drop.load(Ordering::SeqCst);
        if current < inner.store_config.compact_drop_multiple {
            return;
        }

        let mut is_drop = false;
        while current >= inner.store_config.compact_drop_multiple {
            let result = inner.compact_trigger_counts_for_drop.compare_exchange(
                current,
                0,
                Ordering::SeqCst,
                Ordering::SeqCst,
            );
            match result {
                Ok(_) => {
                    is_drop = true;
                    break;
                }
                Err(actual) => {
                    current = actual;
                }
            }
        }

        if !is_drop {
            return;
        }

        let registered_files: FxHashSet<String>;
        let max_registered_fid: u32;
        let mut to_drop_files: FxHashSet<String>;
        let mut to_drop_fids: FxHashSet<u32> = FxHashSet::default();

        {
            let _compact_drop_write_guard = inner.compact_drop_lock.write().unwrap();

            to_drop_files = inner
                .file_operator
                .list(
                    &inner.store_config.base.file_handle_type,
                    Path::new(&inner.store_config.base.local_name_space),
                    false,
                )
                .unwrap_or_else(|e| error_and_panic!("{}", e));

            (registered_files, max_registered_fid) =
                get_registered_index_and_data_files_info(&inner.level_controller);
        }

        Self::get_drop_info(
            inner,
            &mut to_drop_files,
            &mut to_drop_fids,
            &registered_files,
            max_registered_fid,
        );

        drop_files_multi_thread(
            compact_drop_thread_pool,
            &inner.store_config.base.file_handle_type,
            &to_drop_files,
            &inner.store_config.base.local_name_space,
            &inner.file_operator,
            DropFileType::DropLocationDataAfterCompact,
        );

        // Remove fid dropped from file cache and table meta cache.
        for fid in to_drop_fids.iter() {
            inner.fids_to_drop.remove(fid);
            inner.engine_context.cache_handler.file_cache.remove(fid);
            inner
                .engine_context
                .cache_handler
                .table_meta_cache
                .remove(fid);
        }

        s_info!(
            inner.store_config.shard_index,
            "finish dropping index and data files from name space after compaction. index and data files {:?}",
            to_drop_files
        );
    }

    fn get_drop_info(
        inner: &CompactManagerInner,
        to_drop_files: &mut FxHashSet<String>,
        to_drop_fids: &mut FxHashSet<u32>,
        registered_files: &FxHashSet<String>,
        max_registered_fid: u32,
    ) {
        // Remove fid greater than the maximum fid at the beginning of current archive.
        to_drop_files.retain(|file_name| {
            if registered_files.contains(file_name) {
                return false;
            }

            let table_type = get_table_type(file_name);
            if table_type.is_err() {
                return false;
            }
            let table_type = table_type.unwrap();

            if !table_type.is_index_or_data_table() {
                return false;
            }

            let fid: u32 =
                get_integer_from_str_prefix(file_name, DATA_AND_INDEX_FILE_NAME_DELIMITER).unwrap();

            if fid >= max_registered_fid {
                return false;
            }

            if inner.level_controller.is_fid_in_write(fid) {
                return false;
            }

            if table_type.is_index_table() {
                if inner.level_controller.is_fid_in_read(fid) {
                    return false;
                }
            } else if inner.fids_to_drop.contains_key(&fid)
                && inner
                    .level_controller
                    .is_fid_in_read(*inner.fids_to_drop.get(&fid).unwrap())
            {
                return false;
            }

            to_drop_fids.insert(fid);

            true
        });
    }
}
