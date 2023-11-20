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

use rustc_hash::FxHashMap;
use strum_macros::Display;

use super::level_handler::TableInfo;

pub type CompactInfoMap = FxHashMap<u32, CompactInfo>;

#[derive(Default, Debug)]
pub struct InCompactInfoHandler {
    // Fid of index table -> Info of compaction.
    compact_info_map: CompactInfoMap,

    is_in_compact_fragment: bool,
}

// Describe why a file was compacted.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Display)]
pub enum InCompactType {
    #[default]
    None = 0,
    // Files from the highest scoring level are selected for compaction.
    Score = 1,
    // Files that overlap with the previous level for compaction.
    OverLap,
    // Files that neighbor with the previous level for compaction.
    LeftNeighbor,
    // Files that neighbor with the previous level for compaction.
    RightNeighbor,
    // Files that are compacted because of fragments.
    Fragment,
}

#[allow(unused)]
impl InCompactType {
    pub fn is_none(&self) -> bool {
        *self == InCompactType::None
    }

    pub fn is_in_compact(&self) -> bool {
        *self != InCompactType::None
    }

    pub fn is_score(&self) -> bool {
        *self == InCompactType::Score
    }

    pub fn is_overlap(&self) -> bool {
        *self == InCompactType::OverLap
    }

    pub fn is_neighbor(&self) -> bool {
        *self == InCompactType::LeftNeighbor || *self == InCompactType::RightNeighbor
    }

    pub fn is_fragment(&self) -> bool {
        *self == InCompactType::Fragment
    }
}

#[derive(Default, Debug, Clone, Copy)]
pub struct CompactInfo {
    pub in_compact_type: InCompactType,

    pub start: u32,

    pub end: u32,
}

impl InCompactInfoHandler {
    pub fn add_by_compact_type(&mut self, to_add: &[TableInfo], in_compact_type: InCompactType) {
        for item in to_add {
            self.compact_info_map.insert(
                item.index_table_info.fid,
                CompactInfo {
                    in_compact_type,
                    start: item.index_table_info.start,
                    end: item.index_table_info.end,
                },
            );
        }
    }

    pub fn add_by_compact_info(
        &mut self,
        to_add_table_infos: &[TableInfo],
        to_add_compact_infos: Vec<CompactInfo>,
    ) {
        let mut to_add_compact_infos = to_add_compact_infos;

        for item in to_add_table_infos {
            self.compact_info_map
                .insert(item.index_table_info.fid, to_add_compact_infos.remove(0));
        }
    }

    pub fn drop_expired_compact_info(&mut self, to_del_fids: &[u32]) {
        to_del_fids.iter().for_each(|to_del_fid| {
            self.compact_info_map.remove(to_del_fid);
        });
    }

    pub fn get_compact_info_map(&self) -> &CompactInfoMap {
        &self.compact_info_map
    }

    pub fn close(&mut self) {
        self.compact_info_map.clear();
    }

    pub fn is_in_compact_fragment(&self) -> bool {
        self.is_in_compact_fragment
    }

    pub fn set_is_in_compact_fragment(&mut self, is_in_compact_fragment: bool) {
        self.is_in_compact_fragment = is_in_compact_fragment;
    }
}
