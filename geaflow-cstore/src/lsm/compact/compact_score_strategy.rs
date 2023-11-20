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

use crate::{lsm::compact_manager::CompactManagerInner, s_debug};

#[derive(Debug)]
pub struct CompactPriority {
    pub level: usize,
    pub score: f64,
}
pub struct CompactScoreStrategy {}

impl CompactScoreStrategy {
    // TODO: need regularly check ttl file.
    pub fn pick_compact_level(inner: &CompactManagerInner) -> Vec<CompactPriority> {
        let level_size = inner.level_controller.get_level_sizes();
        let mut compact_priority_vec: Vec<CompactPriority> = vec![];

        // calculate level0 prioritized score.
        compact_priority_vec.push(CompactPriority {
            level: 0,
            score: Self::calculate_l0_prioritized_score(inner),
        });

        // calculate other level prioritized score.
        for level in 1..level_size.len() - 1 {
            compact_priority_vec.push(CompactPriority {
                level,
                score: Self::calculate_prioritized_score(inner, level, level_size),
            });
        }

        compact_priority_vec.sort_by(|x, y| y.score.total_cmp(&x.score));

        compact_priority_vec
    }

    fn calculate_l0_prioritized_score(inner: &CompactManagerInner) -> f64 {
        if !inner
            .level_controller
            .get_compact_info_handler(0)
            .read()
            .unwrap()
            .get_compact_info_map()
            .is_empty()
        {
            return 0.0;
        }

        inner
            .level_controller
            .get_level_handler(0)
            .read()
            .unwrap()
            .table_num() as f64
            / inner.store_config.level0_file_num as f64
    }

    fn calculate_prioritized_score(
        inner: &CompactManagerInner,
        level: usize,
        level_size: &[u64],
    ) -> f64 {
        let table_num = inner
            .level_controller
            .get_level_handler(level)
            .read()
            .unwrap()
            .table_num();

        if table_num == 0 {
            return 0.0;
        }

        let mut compacted_size = 0;
        let this_level_size: u64;

        {
            // Get level handler lock firstly.
            let level_handler_read_guard = inner
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

                let compact_info_map = compact_read_lock_guard.get_compact_info_map();

                compact_info_map.iter().for_each(|(fid, _compact_info)| {
                    compacted_size += level_handler_read_guard
                        .get_table_info_map()
                        .get(fid)
                        .unwrap()
                        .index_table_info
                        .uncompressed_size;
                });
            }

            this_level_size = level_handler_read_guard.total_size() - compacted_size;
        }

        let next_level_size = level_size[level + 1];

        let mut score = this_level_size as f64 / next_level_size as f64;

        if score < 1.0
            && table_num as f64
                > inner.store_config.compactor_gc_ratio
                    * inner.store_config.file_size_multiplier as f64
        {
            score = f64::MAX;
            s_debug!(
                inner.store_config.base.shard_index,
                "level {} need to gc table fragments",
                level,
            );
        } else {
            s_debug!(
                inner.store_config.base.shard_index,
                "level {} , this level size {}, this level compact size {}, next level size {}, compact score is {}",
                level,
                this_level_size,
                compacted_size,
                next_level_size,
                this_level_size as f64 / next_level_size as f64
            );
        }

        score
    }
}
