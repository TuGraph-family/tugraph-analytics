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
    collections::HashMap,
    sync::{
        atomic::{AtomicU32, Ordering},
        Arc, Mutex, MutexGuard, RwLock, RwLockReadGuard, RwLockWriteGuard,
    },
};

use dashmap::{DashMap, DashSet};
use rustc_hash::FxHashSet;

use super::compact_info_handler::InCompactInfoHandler;
use crate::{
    config::{StoreConfig, TableConfig},
    file::{file_handle::FileHandleType, file_operator::CstoreFileOperator},
    gen::manifest::{Manifest, TableInfoManifest},
    index::parent_node::IndexMeta,
    lsm::level_handler::{LevelHandler, TableInfo},
    manifest::CopyFileMeta,
    table::build_file_name,
    Result, TableContext, TableType, WritableTable,
};

pub struct LevelController {
    next_file_id: AtomicU32,

    // LevelHandler includes a map from fid of index table to table info, and the total size of
    // index tables in map of table info. Updating the map of table info also requires updating
    // the total size, dashmap is not applicable here, and a read-write lock is required.
    levels: Vec<Arc<RwLock<LevelHandler>>>,

    level_sizes: Vec<u64>,

    compact_info_handlers: Vec<Arc<RwLock<InCompactInfoHandler>>>,

    // Ensure the concurrency safety of archive and compact.
    lsm_update_lock: RwLock<()>,

    // Ensure the concurrency safety of recover and compact.
    recover_lock: RwLock<()>,

    // Ensure the concurrency safety of multi-thread compact.
    compact_lock: Mutex<()>,

    file_operator: Arc<CstoreFileOperator>,

    // Record the fids of index files which are being read.
    fids_in_read: DashMap<u32, u32>,

    // Record the fids of index and data files which are being written.
    fids_in_write: DashSet<u32>,
}

impl LevelController {
    pub fn new(store_config: &StoreConfig, file_operator: Arc<CstoreFileOperator>) -> Self {
        let max_level = store_config.max_level;

        let mut levels = Vec::with_capacity(max_level);
        let mut level_sizes = Vec::with_capacity(max_level);
        let mut compact_info_handlers = Vec::with_capacity(max_level);

        let calculate_level_size = |level: usize| {
            if level == 0 {
                u64::MAX
            } else {
                store_config.level1_file_size
                    * u64::pow(store_config.file_size_multiplier as u64, (level - 1) as u32)
            }
        };

        for level in 0..max_level {
            let level_handler = LevelHandler::new(level as u8, store_config.shard_index);
            levels.push(Arc::new(RwLock::new(level_handler)));

            // Level 0 file flushing depends on the size of the value table, and compaction
            // depends on the number of files in the index table. "0" exists as a
            // placeholder.
            let level_size = if level != 0 {
                calculate_level_size(level)
            } else {
                0
            };

            level_sizes.push(level_size);

            compact_info_handlers.push(Arc::new(RwLock::new(InCompactInfoHandler::default())))
        }

        LevelController {
            next_file_id: AtomicU32::new(0),
            levels,
            level_sizes,
            compact_info_handlers,
            lsm_update_lock: RwLock::new(()),
            recover_lock: RwLock::new(()),
            compact_lock: Mutex::new(()),
            file_operator,
            fids_in_read: DashMap::default(),
            fids_in_write: DashSet::default(),
        }
    }

    pub fn load(
        &self,
        manifest: &Manifest,
        file_handle_type: &FileHandleType,
        persistent_file_handle_type: &FileHandleType,
    ) -> FxHashSet<CopyFileMeta> {
        let mut download_info: FxHashSet<CopyFileMeta> = FxHashSet::default();

        self.next_file_id.store(
            u32::max(manifest.max_registered_fid, manifest.memory_index_fid) + 1,
            Ordering::SeqCst,
        );

        for level in 0..self.max_level() {
            let table_info_map_manifest: &HashMap<u32, TableInfoManifest> = &manifest
                .level_manifest_vec
                .get(level)
                .unwrap()
                .table_info_manifest_map;

            let mut level_handler_write_guard = self.get_level_handler(level).write().unwrap();
            level_handler_write_guard.close();

            let mut copy_index_data_file_info: FxHashSet<CopyFileMeta> = FxHashSet::default();

            // TODO vertex and edge separation, index_file_handle_type,
            // data_file_handle_type
            table_info_map_manifest
                .iter()
                .map(|iter| iter.1)
                .for_each(|table_info_manifest| {
                    let table_info = TableInfo::new_with_manifest(table_info_manifest);
                    let index_table_fid = table_info.index_table_info.fid;
                    let data_table_fid = table_info.data_table_info.fid;

                    // Record the files needed to be copied.
                    let index_table_name = build_file_name(index_table_fid, TableType::Is);
                    let data_table_name = build_file_name(data_table_fid, TableType::Vs);

                    let index_table_cop_meta = CopyFileMeta {
                        file_name: index_table_name,
                        src_file_handle_type: *persistent_file_handle_type,
                        dest_file_handle_type: *file_handle_type,
                    };

                    let data_table_cop_meta = CopyFileMeta {
                        file_name: data_table_name,
                        src_file_handle_type: *persistent_file_handle_type,
                        dest_file_handle_type: *file_handle_type,
                    };

                    copy_index_data_file_info.insert(index_table_cop_meta);
                    copy_index_data_file_info.insert(data_table_cop_meta);

                    level_handler_write_guard
                        .update_tables(Vec::new().as_slice(), vec![table_info].as_slice());
                });

            download_info.extend(copy_index_data_file_info);
        }

        download_info
    }

    /// apply for fid and build table.
    pub fn create_new_table<'a>(
        &'a self,
        table_config: &'a TableConfig,
        table_type: TableType,
    ) -> Result<WritableTable> {
        let fid = self.get_next_fid();
        if table_type.is_index_table() {
            self.add_fids_in_write(&[fid]);
        }

        WritableTable::new(
            table_config,
            TableContext::new(table_config, fid, &self.file_operator).with_table_type(table_type),
        )
    }

    pub fn get_next_fid(&self) -> u32 {
        self.next_file_id.fetch_add(1, Ordering::SeqCst)
    }

    // Register tables to lsm tree.
    pub fn register_to_lsm(&self, to_add_table: &[TableInfo], to_add_level: usize) {
        {
            let mut add_level_handler = self.levels[to_add_level].write().unwrap();
            add_level_handler.add_tables(to_add_table);
        }

        self.delete_fids_in_write(to_add_table);
    }

    // Update tables to lsm tree.
    pub fn update_lsm(&self, to_add_table: &[TableInfo], to_del_fids: &[u32], level: usize) {
        // Get level handler lock firstly.
        let mut level_handler = self.levels[level].write().unwrap();
        {
            let mut del_compact_info_handler = self.compact_info_handlers[level].write().unwrap();

            level_handler.update_tables(to_del_fids, to_add_table);
            del_compact_info_handler.drop_expired_compact_info(to_del_fids);
        }

        self.delete_fids_in_write(to_add_table);
    }

    // Remove tables on lsm tree.
    pub fn delete_lsm(&self, to_del_fids: &[u32], to_del_level: usize) {
        // Get level handler lock firstly.
        let mut del_level_handler = self.levels[to_del_level].write().unwrap();
        {
            let mut del_compact_info_handler =
                self.compact_info_handlers[to_del_level].write().unwrap();

            del_level_handler.delete_tables(to_del_fids);
            del_compact_info_handler.drop_expired_compact_info(to_del_fids);
        }
    }

    pub fn get_level_handler(&self, level: usize) -> &Arc<RwLock<LevelHandler>> {
        &self.levels[level]
    }

    pub fn get_compact_info_handler(&self, level: usize) -> &Arc<RwLock<InCompactInfoHandler>> {
        &self.compact_info_handlers[level]
    }

    pub fn is_readable(&self, fid: u32) -> bool {
        for handler in self.levels.iter() {
            if handler.read().unwrap().contains(fid) {
                return true;
            }
        }
        false
    }

    pub fn max_level(&self) -> usize {
        self.level_sizes.len()
    }

    pub fn close(&self) {
        for level in 0..self.max_level() {
            self.get_level_handler(level).write().unwrap().close();
            self.get_compact_info_handler(level)
                .write()
                .unwrap()
                .close();
        }
    }

    pub fn get_level_sizes(&self) -> &[u64] {
        self.level_sizes.as_slice()
    }

    pub fn scan_lock_guard(&self) -> RwLockReadGuard<()> {
        self.lsm_update_lock.read().unwrap()
    }

    // Function to ensure concurrency safety - Begin.
    pub fn archive_read_guard(&self) -> RwLockReadGuard<()> {
        self.lsm_update_lock.read().unwrap()
    }

    pub fn archive_write_guard(&self) -> RwLockWriteGuard<()> {
        self.lsm_update_lock.write().unwrap()
    }

    pub fn recover_read_guard(&self) -> RwLockReadGuard<()> {
        self.recover_lock.read().unwrap()
    }

    pub fn recover_write_guard(&self) -> RwLockWriteGuard<()> {
        self.recover_lock.write().unwrap()
    }

    pub fn compact_lock_guard(&self) -> MutexGuard<()> {
        self.compact_lock.lock().unwrap()
    }
    // Function to ensure concurrency safety - End.

    pub fn add_fids_in_read(&self, to_add: &[IndexMeta]) {
        to_add.iter().for_each(|index_meta| {
            *self.fids_in_read.entry(index_meta.fid).or_insert(0) += 1;
        });
    }

    pub fn delete_fids_in_read(&self, to_delete: &[IndexMeta]) {
        to_delete.iter().for_each(|index_meta| {
            if self
                .fids_in_read
                .remove_if(&index_meta.fid, |_fid, count| *count == 0)
                .is_none()
            {
                self.fids_in_read.entry(index_meta.fid).and_modify(|count| {
                    *count -= 1;
                });
            }
        });
    }

    pub fn is_fid_in_read(&self, index_table_fid: u32) -> bool {
        self.fids_in_read.contains_key(&index_table_fid)
    }

    pub fn add_fids_in_write(&self, fid: &[u32]) {
        fid.iter().for_each(|fid| {
            self.fids_in_write.insert(*fid);
        });
    }

    pub fn delete_fids_in_write(&self, table_infos: &[TableInfo]) {
        table_infos.iter().for_each(|table_info| {
            self.fids_in_write.remove(&table_info.index_table_info.fid);
        });
    }

    pub fn is_fid_in_write(&self, fid: u32) -> bool {
        self.fids_in_write.contains(&fid)
    }
}
