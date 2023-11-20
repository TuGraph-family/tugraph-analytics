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

use human_bytes::human_bytes;

use crate::{
    common::GraphDataIndex,
    config::StoreConfig,
    index::index_builder::IndexBuilder,
    iterator::segment_iterator::SegmentIter,
    log_util::info,
    lsm::level_handler::{DataTableInfo, IndexTableInfo, TableInfo},
    util::time_util::time_now_as_secs,
    GraphData, Result, WritableTable, SIZE_OF_U32,
};

pub struct LsmFreezer {}

impl LsmFreezer {
    pub fn freeze_sorted_data(
        store_config: &StoreConfig,
        index_builder: &mut IndexBuilder,
        key: &u32,
        graph_data_vec: &[GraphData],
        table: &mut WritableTable,
    ) -> Result<()> {
        let mut property_vec = vec![];
        let mut second_key_vec = vec![];
        let mut single_buffer_size = 0;

        let mut graph_data_iter = graph_data_vec.iter().peekable();
        while let Some(graph_data) = graph_data_iter.next() {
            // don't need clone, data will be cloned to block when calling append function.
            property_vec.push(graph_data.property.as_slice());
            // need copy index.
            second_key_vec.push(graph_data.second_key.clone());
            single_buffer_size += graph_data.property.len();

            if single_buffer_size >= store_config.max_buffer_size_in_segment
                || (graph_data_iter.peek().is_none() && single_buffer_size > 0)
            {
                let is_hot_spot = single_buffer_size >= store_config.max_buffer_size_in_segment;
                let table_offset = table.append_list(&property_vec)?;
                let graph_data_index: GraphDataIndex = GraphDataIndex {
                    graph_data_fid: table.get_fid(),
                    graph_data_offset: table_offset,
                    all_filtered: false,
                    second_keys: second_key_vec,
                };
                index_builder.put(*key, graph_data_index, is_hot_spot)?;

                property_vec = vec![];
                second_key_vec = vec![];
                single_buffer_size = 0;
            }
        }

        Ok(())
    }

    pub fn flush_sorted_data(table: &mut WritableTable) -> Result<()> {
        let start_flush = Instant::now();

        table.finish()?;

        table.close();

        info!(
            "flush compacted data segment cost {}ms",
            start_flush.elapsed().as_millis()
        );

        Ok(())
    }

    pub fn freeze_and_flush_data(
        store_config: &StoreConfig,
        graph_segment_iterator: SegmentIter,
        index_builder: &mut IndexBuilder,
        table: &mut WritableTable,
    ) -> Result<()> {
        let start_flush = Instant::now();

        let mut key_num = 0;

        for (key, graph_data_vec) in graph_segment_iterator {
            key_num += 1;
            let mut property_vec = vec![];
            let mut second_key_vec = vec![];
            let mut single_buffer_size = 0;

            let mut graph_data_iter = graph_data_vec.iter().peekable();

            while let Some(graph_data) = graph_data_iter.next() {
                // don't need clone, data will be cloned to block when calling append function.
                property_vec.push(graph_data.property.as_slice());
                // need copy index.
                second_key_vec.push(graph_data.second_key.clone());
                single_buffer_size += SIZE_OF_U32;
                single_buffer_size += graph_data.property.len();

                if single_buffer_size >= store_config.max_buffer_size_in_segment
                    || (graph_data_iter.peek().is_none() && single_buffer_size > 0)
                {
                    let is_hot_spot = single_buffer_size >= store_config.max_buffer_size_in_segment;
                    if is_hot_spot {
                        info!("freeze hot key {} size {}", key, single_buffer_size);
                    }
                    let table_offset = table.append_list(&property_vec)?;
                    let graph_data_index: GraphDataIndex = GraphDataIndex {
                        graph_data_fid: table.get_fid(),
                        graph_data_offset: table_offset,
                        all_filtered: false,
                        second_keys: second_key_vec,
                    };
                    index_builder.put(*key, graph_data_index, is_hot_spot)?;

                    property_vec = vec![];
                    second_key_vec = vec![];
                    single_buffer_size = 0;
                }
            }
        }

        table.finish()?;
        table.close();

        let value_table_size = table.get_table_offset();

        info!(
            "flush data segment key num {} size {} cost {}ms",
            key_num,
            human_bytes(value_table_size as f64),
            start_flush.elapsed().as_millis()
        );

        Ok(())
    }

    pub fn freeze_index(
        table: &mut WritableTable,
        index_block_bytes: Vec<u8>,
    ) -> Result<(u64, usize)> {
        let index_block_bytes_len = index_block_bytes.len();

        let table_offset: u64 = table.append(index_block_bytes.as_slice())?;

        Ok((table_offset, index_block_bytes_len))
    }

    pub fn flush_index(
        index_table_fid: u32,
        index_table: &mut WritableTable,
        start_end_key: (u32, u32),
        data_table_fid: u32,
        data_table_size: u64,
    ) -> Result<TableInfo> {
        let start_flush = Instant::now();

        index_table.finish()?;

        index_table.close();

        let index_table_size = index_table.get_table_offset();

        let index_table_info = TableInfo {
            index_table_info: IndexTableInfo {
                fid: index_table_fid,
                size: index_table_size,
                uncompressed_size: index_table.get_uncompressed_table_offset(),
                create_ts: time_now_as_secs(),
                start: start_end_key.0,
                end: start_end_key.1,
            },
            data_table_info: DataTableInfo {
                fid: data_table_fid,
                size: data_table_size,
            },
        };

        info!(
            "flush index segment size {} cost {}ms",
            human_bytes(index_table_size as f64),
            start_flush.elapsed().as_millis()
        );

        Ok(index_table_info)
    }
}
