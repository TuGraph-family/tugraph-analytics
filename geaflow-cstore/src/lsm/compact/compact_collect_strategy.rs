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

use itertools::Itertools;

use crate::{
    lsm::{
        compact::compact_score_strategy::CompactScoreStrategy,
        compact_info_handler::{CompactInfo, CompactInfoMap, InCompactType},
        compact_manager::CompactManagerInner,
        level_handler::TableInfo,
    },
    s_debug,
};

#[derive(Debug, Default)]
pub struct KeyRange {
    pub start: u32,
    pub end: u32,
}

impl KeyRange {
    pub fn new(start: u32, end: u32) -> Self {
        KeyRange { start, end }
    }
}

#[derive(Debug)]
#[allow(dead_code)]
pub struct CompactMeta {
    pub is_compact_fragments: bool,
    pub this_level: usize,
    pub this_level_table_infos: Vec<TableInfo>,
    pub this_level_key_range: KeyRange,
    pub next_level: usize,
    pub next_level_table_infos: Vec<TableInfo>,
    pub next_level_key_range: KeyRange,
    pub next_level_size: u64,
    pub next_level_compact_type: InCompactType,
}

pub struct CompactCollectStrategy {}

impl CompactCollectStrategy {
    pub fn pick_compact_meta(inner: &CompactManagerInner) -> Option<CompactMeta> {
        let _compact_lock_guard = inner.level_controller.compact_lock_guard();
        let compact_priority_vec = CompactScoreStrategy::pick_compact_level(inner);

        s_debug!(
            inner.store_config.shard_index,
            "compact priority vec: {:?}",
            &compact_priority_vec
        );

        if compact_priority_vec[0].score < 1.0 {
            return None;
        }

        let mut compact_meta: Option<CompactMeta> = None;

        // Record level which is ban to compact because of table fragment.
        let mut ban_levels: Vec<usize> = vec![];

        for compact_priority in compact_priority_vec.iter() {
            if compact_priority.score < 1.0 {
                break;
            }

            let is_compact_fragments = if compact_priority.score == f64::MAX {
                ban_levels.push(compact_priority.level - 1);
                true
            } else {
                false
            };

            if !is_compact_fragments && ban_levels.contains(&compact_priority.level) {
                break;
            }

            let this_level = compact_priority.level;

            let next_level;
            let (
                this_level_key_range,
                this_level_table_infos,
                next_level_compact_type,
                next_level_table_infos,
            ) = if !is_compact_fragments {
                next_level = this_level + 1;
                Self::collect_table_info_two_levels(inner, this_level)
            } else {
                next_level = this_level;
                (
                    KeyRange::default(),
                    Self::collect_all_table_info_one_level(inner, this_level),
                    InCompactType::Fragment,
                    vec![],
                )
            };

            if this_level_table_infos.is_empty() {
                continue;
            }

            // Build Compact Meta.
            compact_meta = Some(CompactMeta {
                is_compact_fragments,
                this_level,
                this_level_table_infos,
                this_level_key_range,
                next_level,
                next_level_key_range: if next_level_table_infos.is_empty() {
                    KeyRange::default()
                } else {
                    KeyRange::new(
                        next_level_table_infos
                            .first()
                            .unwrap()
                            .index_table_info
                            .start,
                        next_level_table_infos.last().unwrap().index_table_info.end,
                    )
                },
                next_level_table_infos,
                next_level_size: inner.level_controller.get_level_sizes()[next_level],
                next_level_compact_type,
            });
            break;
        }

        compact_meta
    }

    fn collect_table_info_two_levels(
        inner: &CompactManagerInner,
        level: usize,
    ) -> (KeyRange, Vec<TableInfo>, InCompactType, Vec<TableInfo>) {
        let mut this_level_key_range = KeyRange::default();

        let mut this_level_table_infos_vec = Self::collect_this_level_table_infos(inner, level);

        let (next_level_compact_type, next_level_table_infos) =
            Self::collect_next_level_table_infos(
                inner,
                level,
                &mut this_level_table_infos_vec,
                &mut this_level_key_range,
            );

        let this_level_table_infos = if this_level_table_infos_vec.is_empty() {
            vec![]
        } else {
            this_level_table_infos_vec.pop().unwrap()
        };

        (
            this_level_key_range,
            this_level_table_infos,
            next_level_compact_type,
            next_level_table_infos,
        )
    }

    fn collect_this_level_table_infos(
        inner: &CompactManagerInner,
        level: usize,
    ) -> Vec<Vec<TableInfo>> {
        let mut this_level_table_infos: Vec<Vec<TableInfo>> = vec![];

        // Get level handler lock firstly.
        let level_lock_guard = inner
            .level_controller
            .get_level_handler(level)
            .read()
            .unwrap();
        {
            let compact_read_lock_guard = inner
                .level_controller
                .get_compact_info_handler(level)
                .read()
                .unwrap();

            let compact_info_map: &CompactInfoMap = compact_read_lock_guard.get_compact_info_map();

            if level == 0 {
                this_level_table_infos.push(vec![]);
                for table_info in level_lock_guard.get_table_info_vec().iter() {
                    let index_table_fid = table_info.index_table_info.fid;

                    if compact_info_map.get(&index_table_fid).is_none() {
                        this_level_table_infos[0].push((*table_info).clone());
                    }
                }
            } else {
                for table_info in level_lock_guard.get_table_info_vec().iter() {
                    let index_table_fid = table_info.index_table_info.fid;

                    if compact_info_map.get(&index_table_fid).is_none() {
                        this_level_table_infos.push(vec![(*table_info).clone()]);
                    }
                }
            }
        }

        this_level_table_infos
    }

    fn collect_next_level_table_infos(
        inner: &CompactManagerInner,
        level: usize,
        this_level_table_infos_vec: &mut Vec<Vec<TableInfo>>,
        this_level_key_range: &mut KeyRange,
    ) -> (InCompactType, Vec<TableInfo>) {
        #[allow(unused_assignments)]
        let mut next_level_table_infos: Vec<TableInfo> = vec![];
        #[allow(unused_assignments)]
        let mut in_compact_type: InCompactType = InCompactType::None;

        // Get level handler lock firstly.
        let next_level_lock_guard = inner
            .level_controller
            .get_level_handler(level + 1)
            .read()
            .unwrap();

        {
            let mut this_compact_write_lock_guard = inner
                .level_controller
                .get_compact_info_handler(level)
                .write()
                .unwrap();
            let mut next_compact_write_lock_guard = inner
                .level_controller
                .get_compact_info_handler(level + 1)
                .write()
                .unwrap();

            let mut this_level_table_infos_vec_iter = this_level_table_infos_vec.iter().peekable();

            loop {
                let this_level_table_info_vec = this_level_table_infos_vec_iter.next().unwrap();

                let next_level_table_info_vec: Vec<&TableInfo> =
                    next_level_lock_guard.get_table_info_vec();

                this_level_key_range.start = this_level_table_info_vec
                    .first()
                    .unwrap()
                    .index_table_info
                    .start;

                // Level 0 may exist overlap, and need to find max end key.
                this_level_key_range.end = this_level_table_info_vec
                    .iter()
                    .map(|table_info| table_info.index_table_info.end)
                    .max()
                    .unwrap();

                let (in_compact_type_tmp, next_level_table_infos_op, mut compact_infos_op) =
                    Self::find_overlap_or_neighbor_table(
                        level,
                        this_level_key_range,
                        &next_level_table_info_vec,
                        this_compact_write_lock_guard.get_compact_info_map(),
                        next_compact_write_lock_guard.get_compact_info_map(),
                    );

                in_compact_type = in_compact_type_tmp;

                next_level_table_infos = match next_level_table_infos_op {
                    Some(table_infos) => table_infos,
                    None => {
                        if in_compact_type.is_none()
                            || this_level_table_infos_vec_iter.peek().is_none()
                        {
                            this_level_table_infos_vec.clear();
                            return (in_compact_type, vec![]);
                        } else {
                            continue;
                        }
                    }
                };

                next_compact_write_lock_guard
                    .add_by_compact_info(&next_level_table_infos, compact_infos_op.take().unwrap());

                this_compact_write_lock_guard
                    .add_by_compact_type(this_level_table_info_vec, InCompactType::Score);

                if level != 0 {
                    let fid = this_level_table_info_vec[0].index_table_info.fid;
                    this_level_table_infos_vec.retain(|item| item[0].index_table_info.fid == fid);
                }

                break;
            }
        }

        (in_compact_type, next_level_table_infos)
    }

    fn collect_all_table_info_one_level(
        inner: &CompactManagerInner,
        level: usize,
    ) -> Vec<TableInfo> {
        #[allow(unused_assignments)]
        let mut permission = true;
        #[allow(unused_assignments)]
        let mut table_infos = vec![];

        {
            let level_lock_guard = inner
                .level_controller
                .get_level_handler(level - 1)
                .read()
                .unwrap();
            {
                let compact_read_lock_guard = inner
                    .level_controller
                    .get_compact_info_handler(level - 1)
                    .read()
                    .unwrap();

                if !compact_read_lock_guard.is_in_compact_fragment() {
                    let table_info_vec = level_lock_guard.get_table_info_vec();
                    for table_info in table_info_vec {
                        let in_compact_type_tmp_op = compact_read_lock_guard
                            .get_compact_info_map()
                            .get(&table_info.index_table_info.fid);
                        if in_compact_type_tmp_op.is_none() {
                            continue;
                        } else {
                            permission = false;
                            break;
                        }
                    }
                }
            }
        }

        if permission {
            let level_lock_guard = inner
                .level_controller
                .get_level_handler(level)
                .read()
                .unwrap();
            {
                let mut compact_write_lock_guard = inner
                    .level_controller
                    .get_compact_info_handler(level)
                    .write()
                    .unwrap();

                let table_info_vec = level_lock_guard.get_table_info_vec();
                for table_info in table_info_vec {
                    let in_compact_type_tmp_op = compact_write_lock_guard
                        .get_compact_info_map()
                        .get(&table_info.index_table_info.fid);
                    if in_compact_type_tmp_op.is_none() {
                        continue;
                    } else {
                        permission = false;
                        break;
                    }
                }

                table_infos = if permission {
                    let table_infos_tmp = level_lock_guard
                        .get_table_info_vec()
                        .iter()
                        .map(|table_info| (*table_info).clone())
                        .collect_vec();

                    compact_write_lock_guard
                        .add_by_compact_type(&table_infos_tmp, InCompactType::Fragment);
                    compact_write_lock_guard.set_is_in_compact_fragment(true);

                    table_infos_tmp
                } else {
                    vec![]
                };
            }
        }

        table_infos
    }

    // Find the files in next level which overlap or neighbor with the selected
    // files in this level.
    //
    // Return: the files in next level which is need to be compacted with the
    //         selected files in this level.
    //         Some() => It's needed to compact, None => It's no need to compact.
    //
    // 1. Choose files that overlap with the files in this level preferentially.
    // 2. Choose the closest neighbor file if there are no overlapping files in the next level.
    // 3. If the overlapping files are being compacted, return None.
    //
    fn find_overlap_or_neighbor_table(
        level: usize,
        key_range: &KeyRange,
        table_infos: &[&TableInfo],
        this_level_compact_info_map: &CompactInfoMap,
        next_level_compact_info_map: &CompactInfoMap,
    ) -> (
        InCompactType,
        Option<Vec<TableInfo>>,
        Option<Vec<CompactInfo>>,
    ) {
        // the files in next level that overlap with the files in this level.
        let mut overlap_or_neighbor_table_infos: Vec<TableInfo> = vec![];
        let mut compact_infos: Vec<CompactInfo> = vec![];
        let mut in_compact_type = InCompactType::None;

        // If there is no file in the next level, the files selected to compact in this
        // level needs to be moved to the next level.
        if table_infos.is_empty() {
            for (_fid, compact_info) in this_level_compact_info_map.iter() {
                if compact_info.in_compact_type.is_score() {
                    return (in_compact_type, None, None);
                }
            }

            return (
                in_compact_type,
                Some(overlap_or_neighbor_table_infos),
                Some(compact_infos),
            );
        } else {
            let mut is_table_not_in_compact = false;
            for table_info in table_infos.iter() {
                if next_level_compact_info_map
                    .get(&table_info.index_table_info.fid)
                    .is_none()
                {
                    is_table_not_in_compact = true;
                    break;
                }
            }

            if !is_table_not_in_compact && level != 0 {
                return (in_compact_type, None, None);
            }
        }

        // The closest left neighbor file to the files in this level in next level.
        let mut _left_neighbor_index = usize::MAX;
        // The closest right neighbor file to the files in this level in next level.
        let mut _right_neighbor_index = usize::MAX;

        let start = key_range.start;
        let end = key_range.end;

        // Choose files that overlap with the files in this level preferentially.
        for (index, table_info) in table_infos.iter().enumerate() {
            let table_info_tmp = &table_info.index_table_info;
            let fid_tmp = table_info_tmp.fid;

            let (start_tmp, end_tmp) = match next_level_compact_info_map.get(&fid_tmp) {
                Some(compact_info) => (compact_info.start, compact_info.end),
                None => (table_info_tmp.start, table_info_tmp.end),
            };

            if end < start_tmp {
                _right_neighbor_index = index;
                break;
            } else if start > end_tmp {
                _left_neighbor_index = index;
            } else {
                in_compact_type = InCompactType::OverLap;

                // If the overlapping files are being compacted, return None.
                if next_level_compact_info_map.get(&fid_tmp).is_some() {
                    return (in_compact_type, None, None);
                }

                overlap_or_neighbor_table_infos.push((*table_info).clone());

                compact_infos.push(CompactInfo {
                    in_compact_type: InCompactType::OverLap,
                    start: u32::min(start_tmp, start),
                    end: u32::max(end_tmp, end),
                });
            }
        }

        // Choose the closest neighbor file if there are no overlapping files in the
        // next level, left(upper boundary) first and right(lower boundary) upper
        // second.
        if overlap_or_neighbor_table_infos.is_empty() {
            if _left_neighbor_index != usize::MAX {
                let left_table_info_tmp = table_infos.get(_left_neighbor_index).unwrap();
                in_compact_type = InCompactType::LeftNeighbor;

                if next_level_compact_info_map
                    .get(&left_table_info_tmp.index_table_info.fid)
                    .is_none()
                {
                    overlap_or_neighbor_table_infos.push((*left_table_info_tmp).clone());

                    compact_infos.push(CompactInfo {
                        in_compact_type: InCompactType::LeftNeighbor,
                        start: left_table_info_tmp.index_table_info.start,
                        end,
                    });
                }
            }

            if _right_neighbor_index != usize::MAX && overlap_or_neighbor_table_infos.is_empty() {
                let right_table_info_tmp = table_infos.get(_right_neighbor_index).unwrap();
                in_compact_type = InCompactType::RightNeighbor;

                if next_level_compact_info_map
                    .get(&right_table_info_tmp.index_table_info.fid)
                    .is_none()
                {
                    overlap_or_neighbor_table_infos.push((*right_table_info_tmp).clone());

                    compact_infos.push(CompactInfo {
                        in_compact_type: InCompactType::RightNeighbor,
                        start,
                        end: right_table_info_tmp.index_table_info.end,
                    });
                }
            }
        }

        if overlap_or_neighbor_table_infos.is_empty() && level != 0 {
            // For level 0, as long as the next level (level 1) has no overlapping files,
            // Some is returned, allowing compaction. For other levels, if no file is
            // selected in the next level, None is returned and this compaction is
            // abandoned.
            return (in_compact_type, None, None);
        }

        assert_ne!(in_compact_type, InCompactType::None);

        (
            in_compact_type,
            Some(overlap_or_neighbor_table_infos),
            Some(compact_infos),
        )
    }
}
