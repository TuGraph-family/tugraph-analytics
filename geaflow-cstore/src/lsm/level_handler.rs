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

use std::{fmt, fmt::Formatter};

use itertools::Itertools;
use rustc_hash::FxHashMap;

use crate::{gen::manifest::TableInfoManifest, s_info};

pub type TableInfoMap = FxHashMap<u32, TableInfo>;

#[derive(Clone, Debug)]
pub struct TableInfo {
    pub index_table_info: IndexTableInfo,

    pub data_table_info: DataTableInfo,
}

impl TableInfo {
    pub fn new_with_manifest(table_info_manifest: &TableInfoManifest) -> Self {
        let table_info = TableInfo {
            index_table_info: IndexTableInfo {
                fid: table_info_manifest
                    .index_table_manifest
                    .as_ref()
                    .unwrap()
                    .fid,

                size: table_info_manifest
                    .index_table_manifest
                    .as_ref()
                    .unwrap()
                    .size,

                uncompressed_size: table_info_manifest
                    .index_table_manifest
                    .as_ref()
                    .unwrap()
                    .uncompressed_size,

                create_ts: table_info_manifest
                    .index_table_manifest
                    .as_ref()
                    .unwrap()
                    .create_ts,

                start: table_info_manifest
                    .index_table_manifest
                    .as_ref()
                    .unwrap()
                    .start,

                end: table_info_manifest
                    .index_table_manifest
                    .as_ref()
                    .unwrap()
                    .end,
            },

            data_table_info: DataTableInfo {
                fid: table_info_manifest
                    .data_table_manifest
                    .as_ref()
                    .unwrap()
                    .fid,
                size: table_info_manifest
                    .data_table_manifest
                    .as_ref()
                    .unwrap()
                    .size,
            },
        };

        table_info
    }
}

#[derive(Clone, Debug)]
pub struct IndexTableInfo {
    pub fid: u32,

    pub size: u64,

    pub uncompressed_size: u64,

    pub create_ts: u64,

    /// table start key.
    pub start: u32,

    /// table end key.
    pub end: u32,
}

#[derive(Clone, Debug)]
pub struct DataTableInfo {
    pub fid: u32,

    pub size: u64,
}

impl fmt::Display for TableInfo {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "IndexTable[size {} uncompressed_size {} create_ts {} start {} end {}] DataTable[fid {} size {}]",
            self.index_table_info.size,
            self.index_table_info.uncompressed_size,
            self.index_table_info.create_ts,
            self.index_table_info.start,
            self.index_table_info.end,
            self.data_table_info.fid,
            self.data_table_info.size
        )
    }
}

pub struct LevelHandler {
    // An index table corresponds to a value table.
    // Fid of index table -> TableInfo
    table_info_map: TableInfoMap,

    level: u8,

    total_size: u64,

    shard_index: u32,
}

impl LevelHandler {
    pub fn new(level: u8, shard_index: u32) -> Self {
        Self {
            table_info_map: FxHashMap::default(),
            level,
            total_size: 0,
            shard_index,
        }
    }

    pub fn update_tables(&mut self, to_del_fids: &[u32], to_add_tables: &[TableInfo]) {
        to_del_fids.iter().for_each(|to_del_fid| {
            let del_table = self.table_info_map.remove(to_del_fid);

            // Update the total size of index tables in this level.
            if let Some(del_table) = del_table {
                self.total_size -= del_table.index_table_info.uncompressed_size;
            }
        });

        to_add_tables.iter().for_each(|to_add_table| {
            self.table_info_map
                .insert(to_add_table.index_table_info.fid, to_add_table.clone());

            // Update the total size of index tables in this level.
            self.total_size += to_add_table.index_table_info.uncompressed_size;
        });

        s_info!(
            self.shard_index,
            "level {} delete table {} add table {} remaining table {}",
            self.level,
            to_del_fids.len(),
            to_add_tables.len(),
            self.table_info_map.len()
        )
    }

    pub fn delete_tables(&mut self, to_del_fids: &[u32]) {
        to_del_fids.iter().for_each(|to_del_fid| {
            let del_table = self.table_info_map.remove(to_del_fid);

            // Update the total size of index tables in this level.
            if let Some(del_table) = del_table {
                self.total_size -= del_table.index_table_info.uncompressed_size;
            }
        });

        s_info!(
            self.shard_index,
            "level {} delete table {}, remaining table {}",
            self.level,
            to_del_fids.len(),
            self.table_info_map.len()
        )
    }

    pub fn add_tables(&mut self, to_add_tables: &[TableInfo]) {
        to_add_tables.iter().for_each(|to_add_table| {
            self.table_info_map
                .insert(to_add_table.index_table_info.fid, to_add_table.clone());

            // Update the total size of index tables in this level.
            self.total_size += to_add_table.index_table_info.uncompressed_size;
        });

        s_info!(
            self.shard_index,
            "level {} add table {} remaining table {}",
            self.level,
            to_add_tables.len(),
            self.table_info_map.len()
        )
    }

    pub fn get_table_info_map(&self) -> &TableInfoMap {
        &self.table_info_map
    }

    pub fn get_table_info_vec(&self) -> Vec<&TableInfo> {
        let mut table_info_vec = self.table_info_map.iter().map(|item| item.1).collect_vec();
        table_info_vec.sort_by(|x, y| x.index_table_info.start.cmp(&y.index_table_info.start));
        table_info_vec
    }

    pub fn table_num(&self) -> usize {
        self.table_info_map.len()
    }

    pub fn contains(&self, fid: u32) -> bool {
        self.table_info_map.contains_key(&fid)
    }

    pub fn close(&mut self) {
        self.table_info_map.clear();
        self.total_size = 0;
    }

    pub fn total_size(&self) -> u64 {
        self.total_size
    }
}
