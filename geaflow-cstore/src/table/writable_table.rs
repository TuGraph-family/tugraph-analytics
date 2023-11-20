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

use std::{mem::swap, path::PathBuf};

use bytes::BufMut;
use prost::Message;

use super::get_upper_bound;
use crate::{
    common::{
        gen::meta::{BlockMeta, TableMeta},
        BLOCK_RECORD_LENGTH_OFFSET_USIZE, BLOCK_RECORD_LENGTH_SIZE_U32,
        BLOCK_RECORD_NUM_OFFSET_USIZE, BLOCK_RECORD_NUM_SIZE_U32,
        PROPERTY_META_LENGTH_OFFSET_USIZE,
    },
    config::TableConfig,
    convert_u64_to_u32, convert_usize_to_u32,
    file::file_handle::{FileHandle, OpenMode},
    s_info,
    table::get_table_name_space,
    util::compress_util::{compress, CompressHandler, CompressType},
    Result, TableContext,
};

/// Writable Table.
pub struct WritableTable<'a> {
    /// File handle, write files on disk through it.
    file: Box<dyn FileHandle>,

    /// Table state, to avoid repeated cleaning of the table.
    closed: bool,

    /// Data total length and cur offset of the file on disk.
    table_physical_offset: u64,

    /// Cur block num and index on this table.
    block_num: u32,

    /// Block writer, including the block itself, block-related variable.
    block_writer: BlockWriter<'a>,

    /// List of offset meta for each block.
    block_meta_list: Vec<BlockMeta>,

    /// File id.
    fid: u32,
}

/// Writable block.
pub struct BlockWriter<'a> {
    /// Table context, including compress type, table type and fid.
    table_context: TableContext<'a>,

    /// Table config, include static config and option info.
    table_config: &'a TableConfig,

    compress_handler: CompressHandler,

    /// Block that engine the raw data.
    raw_buffer: Vec<u8>,

    /// Block that engine the compressed data.
    compressed_buffer: Vec<u8>,

    /// Cur position to to be written.
    position: u32,

    /// Last position written.
    pre_position: u32,

    /// Position of data uncompressed.
    uncompressed_position: u64,

    /// Compressed length of the data on the block.
    compressed_length: u32,

    /// Each Block may trigger a grow, and the size after grow is
    /// block_max_size.
    block_max_size: u32,
}

impl<'a> WritableTable<'a> {
    pub fn new(
        table_config: &'a TableConfig,
        table_context: TableContext<'a>,
    ) -> Result<WritableTable<'a>> {
        let file_handle_type = table_context.file_handle_type;
        let table_name_space = get_table_name_space(&file_handle_type, &table_config.base);
        let fid = table_context.fid;

        // Concatenate file path.
        let file_path = PathBuf::new()
            .join(table_name_space)
            .join(fid.to_string())
            .with_extension(table_context.table_type.to_string().to_ascii_lowercase());

        let block_size = table_config.block_size;

        let compressed_block_size = snap::raw::max_compress_len(block_size as usize);

        // Build the file.
        let file_operator = &table_context.file_operator;
        file_operator.create_file(&file_handle_type, &file_path)?;

        let file = file_operator.open(&file_handle_type, &file_path, OpenMode::WriteOnly)?;
        let compress_handler = CompressHandler::new(table_context.compress_type);

        s_info!(
            table_config.base.shard_index,
            "create writable table path {}",
            file_path.clone().to_str().unwrap()
        );

        // Build the writable block.
        let block_writer: BlockWriter<'a> = BlockWriter {
            table_context,
            table_config,
            compress_handler,
            raw_buffer: Vec::with_capacity(block_size as usize),
            compressed_buffer: vec![0u8; compressed_block_size],
            position: 0,
            pre_position: 0,
            uncompressed_position: 0,
            compressed_length: 0,
            block_max_size: block_size,
        };

        Ok(WritableTable {
            file,
            closed: false,
            table_physical_offset: 0,
            block_num: 0,
            block_writer,
            block_meta_list: Vec::new(),
            fid,
        })
    }

    /// Append data to table.
    /// 1. Estimate the length of the data to be stuffed into the block.
    /// 2. Determine whether cur block needs to be persisted.
    /// 3. Append the data to the cur block, and return block offset on the table.
    pub fn append(&mut self, val: &[u8]) -> Result<u64> {
        let estimated_size = self.block_writer.estimated_size(val);

        if self.block_writer.should_flush(estimated_size, false) {
            self.flush_data()?;
        }

        Ok(self.block_writer.append(val, estimated_size))
    }

    /// Append data list to table.
    pub fn append_list(&mut self, val: &Vec<&[u8]>) -> Result<u64> {
        let estimated_size = self.block_writer.estimated_size_list(val);

        if self.block_writer.should_flush(estimated_size, false) {
            self.flush_data()?;
        }

        Ok(self.block_writer.append_list(val, estimated_size))
    }

    /// Flush data, meta and checksum to disk
    pub fn finish(&mut self) -> Result<()> {
        // Flush data.
        self.flush_data()?;

        // Flush meta of the table.
        self.flush_meta()?;

        // Flush the checksum.
        self.flush_checksum();

        Ok(())
    }

    /// Flush data to disk.
    /// 1.Build (Compress).
    /// 2.Flush data into disk.
    /// 3.Record meta of cur block.
    /// 4.Update meta list, reset block and Update the physical offset on disk
    fn flush_data(&mut self) -> Result<()> {
        // Compress(Build).
        let raw_buffer = &self.block_writer.raw_buffer;
        let compressed_buffer = &mut self.block_writer.compressed_buffer;

        self.block_writer.compressed_length = convert_usize_to_u32!(compress(
            &mut self.block_writer.compress_handler,
            raw_buffer,
            compressed_buffer
        ));

        let position = self.block_writer.position;
        let block_size = self.block_writer.table_config.block_size;

        self.block_writer.uncompressed_position += get_upper_bound(block_size, position) as u64;

        // Get compressed & uncompressed length after build.
        let uncompressed_length = position;
        let compressed_length = self.block_writer.compressed_length;

        if uncompressed_length == 0 {
            return Ok(());
        }

        // Flush data into disk.
        self.file
            .write(&self.block_writer.compressed_buffer[..compressed_length as usize])?;

        // Record meta of cur block and put it into the list of meta.
        self.block_meta_list.push(BlockMeta {
            table_offset: self.table_physical_offset,
            uncompressed_length,
            compressed_length,
        });

        // Reset block and Update the physical offset on disk
        self.table_physical_offset += compressed_length as u64;
        self.block_writer.reset();
        self.block_num += 1;

        Ok(())
    }

    /// Flush table meta.
    /// 1. Build table meta.
    /// 2. Serialize table's meta, include meta data and length.
    /// 3. Flush table's meta into disk.
    fn flush_meta(&mut self) -> Result<()> {
        let compress_type_value = CompressType::into(self.block_writer.table_context.compress_type);
        let block_meta = &mut self.block_meta_list;

        // Use swap to avoid unnecessary cloning of variables.
        let mut block_meta_tmp: Vec<BlockMeta> = Vec::new();
        swap(block_meta, &mut block_meta_tmp);

        // Build table meta.
        let table_meta = TableMeta {
            data_block_size: self.block_writer.table_config.block_size,
            compress_type: compress_type_value,
            block_meta: block_meta_tmp,
        };

        // Serialize table's meta, include meta data and length.
        let mut meta_buf =
            Vec::with_capacity(table_meta.encoded_len() + BLOCK_RECORD_NUM_OFFSET_USIZE);
        table_meta.encode(&mut meta_buf)?;

        meta_buf.put_u32(convert_usize_to_u32!(meta_buf.len()));

        // Flush table's meta into disk.
        self.file.write(&meta_buf)?;

        // Update the physical offset of cur table.
        self.table_physical_offset += meta_buf.len() as u64;

        Ok(())
    }

    /// Flush the checksum.
    pub fn flush_checksum(&mut self) {
        // TODO
    }

    /// Close file.
    pub fn close(&mut self) {
        if !self.closed {
            self.closed = true;
        }
    }

    /// Get the table offset in disk.
    pub fn get_table_offset(&self) -> u64 {
        self.table_physical_offset
    }

    /// Get the table offset in memory.
    pub fn get_uncompressed_table_offset(&self) -> u64 {
        self.block_writer.uncompressed_position
    }

    pub fn get_fid(&self) -> u32 {
        self.fid
    }
}

impl<'a> BlockWriter<'a> {
    /// Append the data to the cur block, and return block offset on the table.
    pub fn append(&mut self, val: &[u8], estimated_size: u32) -> u64 {
        if self.should_grow(estimated_size) {
            self.grow(estimated_size);
        }

        // Update pre position.
        self.pre_position = self.position;
        self.raw_buffer.put_u32(1);
        self.position += BLOCK_RECORD_NUM_SIZE_U32;

        // Append length of the value.
        let val_len = convert_usize_to_u32!(val.len());
        self.raw_buffer.put_u32(val_len);

        self.position += BLOCK_RECORD_LENGTH_SIZE_U32;

        // Append value.
        self.raw_buffer.put(val);
        self.position += val_len;

        // Return block offset on the table.
        self.uncompressed_position + self.pre_position as u64
    }

    /// Append the data list to the cur block, and return block offset on the
    /// table.
    pub fn append_list(&mut self, val: &Vec<&[u8]>, estimated_size: u32) -> u64 {
        if self.should_grow(estimated_size) {
            self.grow(estimated_size);
        }

        // Update pre position.
        self.pre_position = self.position;

        // Append length of the value vector
        self.raw_buffer.put_u32(convert_usize_to_u32!(val.len()));
        self.position += BLOCK_RECORD_NUM_SIZE_U32;

        for item in val {
            // Append length of the value.
            let item_len = convert_usize_to_u32!(item.len());
            self.raw_buffer.put_u32(item_len);
            self.position += BLOCK_RECORD_LENGTH_SIZE_U32;

            // Append value.
            self.raw_buffer.put(*item);
            self.position += item_len;
        }

        // Return block offset on the table.
        self.uncompressed_position + self.pre_position as u64
    }

    /// Estimate the size whether enough to append the data.
    pub fn estimated_size(&self, data: &[u8]) -> u32 {
        // len(offset)+ len(data_len) + len(data)
        convert_usize_to_u32!(PROPERTY_META_LENGTH_OFFSET_USIZE + data.len())
    }

    /// Estimate the size whether enough to append the data list.
    pub fn estimated_size_list(&self, data: &Vec<&[u8]>) -> u32 {
        // init with len(offset)
        let mut estimated_size = BLOCK_RECORD_NUM_OFFSET_USIZE;

        // Add len(data_len) and len(data)
        for item in data {
            estimated_size += BLOCK_RECORD_LENGTH_OFFSET_USIZE;
            estimated_size += item.len();
        }

        convert_usize_to_u32!(estimated_size)
    }

    /// Determine whether flush block into disk.
    pub fn should_flush(&self, estimated_size: u32, force: bool) -> bool {
        let block_size = self.table_config.block_size;
        let almost_full_pos =
            (self.table_config.block_capacity_critical_ratio_percentage * block_size) / 100;

        if self.position == 0 {
            false
        } else {
            (force || estimated_size + self.position > block_size)
                && self.position > almost_full_pos
        }
    }

    /// Determine whether expand the block.
    pub fn should_grow(&self, estimated_size: u32) -> bool {
        estimated_size + self.position > self.block_max_size
    }

    pub fn grow(&mut self, estimated_size: u32) {
        let position = self.position;
        let block_size = self.table_config.block_size;

        let desired_size = if (estimated_size + position) % block_size == 0 {
            estimated_size + position
        } else {
            ((estimated_size + position) / block_size + 1) * block_size
        };

        let desired_size_next_power_of_two = desired_size.next_power_of_two() as u64;
        let block_max_size_tmp = desired_size_next_power_of_two + desired_size_next_power_of_two;

        let block_size_max_value = self.table_config.block_size_max_value as u64;

        self.block_max_size = if block_max_size_tmp > block_size_max_value {
            convert_u64_to_u32!(block_size_max_value)
        } else {
            convert_u64_to_u32!(block_max_size_tmp)
        };

        let block_max_size = self.block_max_size as usize;

        self.raw_buffer.reserve(block_max_size - position as usize);

        self.compressed_buffer
            .resize(snap::raw::max_compress_len(block_max_size), 0);
    }

    /// Reset. It need to be executed after cur block flush data.
    pub fn reset(&mut self) {
        self.position = 0;
        self.raw_buffer.clear();
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use strum_macros::{Display, EnumString};

    use crate::{
        config::{CStoreConfig, CStoreConfigBuilder},
        engine::dict::label_dict::LabelDict,
        log_util::{self, info, LogLevel, LogType},
        test_util::{
            bind_job_name_with_ts, delete_test_dir, test_table_append, test_table_read_randomly,
            test_table_read_sequentially, GraphDataGenerator, GraphDataGeneratorParameters,
            TableTestParameters,
        },
        ConfigMap, EngineContext,
    };

    #[derive(PartialEq, Display, EnumString)]
    enum TestMode {
        LocalAppend = 1,
        LocalAppendList,
    }

    fn build_test_parameters(
        test_mode: &TestMode,
        test_name: &str,
    ) -> (
        CStoreConfig,
        GraphDataGeneratorParameters,
        TableTestParameters,
    ) {
        let mut config_map = ConfigMap::default();

        let job_name = bind_job_name_with_ts(test_name);
        config_map.insert("store.job_name", job_name.as_str());

        let cstore_config: CStoreConfig = CStoreConfigBuilder::default()
            .set_with_map(&config_map)
            .build();

        let property_len_interval_vec: Vec<(usize, usize)> = match test_mode {
            TestMode::LocalAppend | TestMode::LocalAppendList => vec![
                (0, 1),
                (1, 50),
                (50, 100),
                (100, 1000),
                (1000, 5000),
                (5000, 25000),
            ],
        };

        let graph_data_gen_parameters = GraphDataGeneratorParameters::default()
            .with_random_switch(true)
            .with_target_id_len_interval_vec(vec![(4, 5)])
            .with_property_len_interval_vec(property_len_interval_vec);

        let table_test_parameters = TableTestParameters::default()
            .with_flush_table_size(4194304)
            .with_flush_table_size(4194304);

        (
            cstore_config,
            graph_data_gen_parameters,
            table_test_parameters,
        )
    }

    // 1. Append and flush data on WritableTable.
    // 2. Randomly read data on ReadableTable.
    // 3. Sequentially read block on ReadableTable.
    fn test_table(test_mode: &TestMode) {
        log_util::try_init(LogType::ConsoleAndFile, LogLevel::Debug, 0);

        let (cstore_config, graph_data_gen_parameters, table_test_parameters) =
            build_test_parameters(test_mode, "test_table_ut");
        let table_config = &cstore_config.table;

        let label_dict = Arc::new(LabelDict::default());

        // New a generator and Generate graphdatas of specific amount.
        let mut graph_generator = GraphDataGenerator::new(&graph_data_gen_parameters);
        let graph_data_vec_triad_vec = graph_generator.generate();

        for i in table_test_parameters.iterations_vec.iter() {
            // Test Table Append Single data & Read
            for (index, graph_data_vec_triad) in graph_data_vec_triad_vec.iter().enumerate() {
                let graph_data_vec = &graph_data_vec_triad.2;
                let property_len_interval = graph_data_gen_parameters
                    .property_len_interval_vec
                    .get(index)
                    .unwrap();
                info!(
                    "start new round test, with random property len in interval [{}, {})",
                    property_len_interval.0, property_len_interval.1
                );

                let engine_context = Arc::new(EngineContext::new(&label_dict, &cstore_config));

                // Test table append, return a Vec<triple>, which describes [fid, block offset,
                // reference of block data] for each item.
                let vec_fid_offset = test_table_append(
                    &cstore_config.table,
                    graph_data_vec,
                    1,
                    *i,
                    table_test_parameters.flush_table_size,
                    &engine_context.file_operator,
                );

                // Test table read sequentially.
                test_table_read_sequentially(
                    &cstore_config.table,
                    &engine_context.cache_handler,
                    &vec_fid_offset,
                    &engine_context.file_operator,
                );

                // Test table read randomly.
                test_table_read_randomly(
                    &cstore_config.table,
                    &engine_context.cache_handler,
                    &vec_fid_offset,
                    table_test_parameters.multi_read_thread_num,
                    &engine_context.file_operator,
                );

                delete_test_dir(&table_config, &cstore_config.persistent);
            }
        }
    }

    // 1. Append List and flush data on WritableTable.
    // 2. Randomly read data on ReadableTable.
    // 3. Sequentially read block on ReadableTable.
    fn test_table_list(test_mode: &TestMode) {
        log_util::try_init(LogType::ConsoleAndFile, LogLevel::Debug, 0);

        let (cstore_config, graph_data_gen_parameters, table_test_parameters) =
            build_test_parameters(test_mode, "test_table_list_ut");
        let table_config = &cstore_config.table;
        let label_dict = Arc::new(LabelDict::default());

        // New a generator and Generate graphdatas of specific amount.
        let mut graph_generator = GraphDataGenerator::new(&graph_data_gen_parameters);
        let mut graph_data_vec_triad_vec = graph_generator.generate();

        // Test Table Append list with less data.
        graph_data_vec_triad_vec.pop();

        for i in table_test_parameters.iterations_vec.iter() {
            // Test Table Append Single data & Read
            for (index, graph_data_vec_triad) in graph_data_vec_triad_vec.iter().enumerate() {
                let graph_data_vec = &graph_data_vec_triad.2;

                let property_len_interval = graph_data_gen_parameters
                    .property_len_interval_vec
                    .get(index)
                    .unwrap();
                info!(
                    "start new round test, with random property len in interval [{}, {})",
                    property_len_interval.0, property_len_interval.1
                );

                for property_len in table_test_parameters.property_num_vec.iter() {
                    let engine_context: Arc<EngineContext> =
                        Arc::new(EngineContext::new(&label_dict, &cstore_config));

                    // Test table append, return a Vec<triple>, which describes [fid, block offset,
                    // reference of block data] for each item.
                    let vec_fid_offset = test_table_append(
                        &cstore_config.table,
                        graph_data_vec,
                        *property_len,
                        *i,
                        table_test_parameters.flush_table_size,
                        &engine_context.file_operator,
                    );

                    // Test table read sequentially.
                    test_table_read_sequentially(
                        &cstore_config.table,
                        &engine_context.cache_handler,
                        &vec_fid_offset,
                        &engine_context.file_operator,
                    );

                    // Test table read randomly.
                    test_table_read_randomly(
                        &cstore_config.table,
                        &engine_context.cache_handler,
                        &vec_fid_offset,
                        table_test_parameters.multi_read_thread_num,
                        &engine_context.file_operator,
                    );

                    delete_test_dir(&table_config, &cstore_config.persistent);
                }
            }
        }
    }

    #[test]
    pub fn test_table_append_local() {
        test_table(&TestMode::LocalAppend);
    }

    #[test]
    pub fn test_table_append_list_local() {
        test_table_list(&TestMode::LocalAppendList);
    }
}
