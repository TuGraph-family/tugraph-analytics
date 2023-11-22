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

use std::{path::PathBuf, sync::Arc};

use rustc_hash::FxHashMap;

use super::{block::Block, get_table_name_space, get_upper_bound};
use crate::{
    cache::{
        block_cache::{BlockCache, BlockInitParameter},
        file_cache::{FileHandlePoolRefOp, FileRefOp},
        table_meta_cache::{TableMetaBlock, TableMetaCache},
        CStoreCacheHandler,
    },
    common::{
        gen::meta::BlockMeta, BLOCK_RECORD_LENGTH_OFFSET_USIZE, BLOCK_RECORD_NUM_OFFSET_USIZE,
    },
    config::TableConfig,
    convert_two_u32_to_u64, convert_u64_to_u32, convert_usize_to_u32,
    error::Error,
    error_and_panic,
    file::file_handle::OpenMode,
    get_u32_from_bytes,
    util::compress_util::{decompress, DecompressHandler},
    CompressType, Result, TableContext,
};

/// Readable table.
pub struct ReadableTable<'a> {
    // file id.
    fid: u32,

    // File handle, read files on disk through it.
    file: FileRefOp,

    // File handle pool.
    file_pool: FileHandlePoolRefOp,

    // Table state, to avoid repeated cleaning of the table.
    closed: bool,

    // Block reader, including the block itself, block-related variable.
    block_reader: BlockReader<'a>,

    // TableMeta of the table.
    table_meta_block: Option<Arc<TableMetaBlock>>,

    // Block size, get from table meta.
    block_size: u32,

    // Cache handle.
    cache_handler: &'a CStoreCacheHandler,
}

/// Readable block.
pub struct BlockReader<'a> {
    /// Table context, including compress type, table type and fid.
    table_context: TableContext<'a>,

    /// Decompress handle
    decompress_handler: DecompressHandler,
}

impl<'a> ReadableTable<'a> {
    pub fn new(
        table_config: &'a TableConfig,
        table_context: TableContext<'a>,
        cache_handler: &'a CStoreCacheHandler,
    ) -> Result<ReadableTable<'a>> {
        let fid = table_context.fid;

        // Read the file handle from filehandle cache or disk
        let file_handle_type = &table_context.file_handle_type;

        let table_name_space = get_table_name_space(file_handle_type, &table_config.base);

        let mut file_pool: FileHandlePoolRefOp = FileHandlePoolRefOp::default();

        let file_operator = &table_context.file_operator;

        let file: FileRefOp = match cache_handler.file_cache.capacity() > 0 {
            true => {
                file_pool = FileHandlePoolRefOp::new(cache_handler.file_cache.get_with(&fid)?);

                match file_pool.pop() {
                    Some(file) => FileRefOp::new_with_ref(file),

                    None => {
                        // Concatenate file path.
                        let file_path = PathBuf::new()
                            .join(table_name_space)
                            .join(fid.to_string())
                            .with_extension(
                                table_context.table_type.to_string().to_ascii_lowercase(),
                            );

                        let file =
                            file_operator.open(file_handle_type, &file_path, OpenMode::ReadOnly)?;

                        FileRefOp::new(file)
                    }
                }
            }

            false => {
                // Concatenate file path.
                let file_path = PathBuf::new()
                    .join(table_name_space)
                    .join(fid.to_string())
                    .with_extension(table_context.table_type.to_string().to_ascii_lowercase());

                let file = file_operator.open(file_handle_type, &file_path, OpenMode::ReadOnly)?;

                FileRefOp::new(file)
            }
        };

        // Read meta of the table from tablemeta cache or disk
        let table_meta_block = match cache_handler.table_meta_cache.capacity() > 0 {
            true => cache_handler
                .table_meta_cache
                .get_with(&table_context.fid, &file),
            false => TableMetaCache::get_table_meta_block(&file),
        }?;

        let block_size = table_meta_block.table_meta.data_block_size;
        let compress_type = CompressType::try_from(table_meta_block.table_meta.compress_type)
            .map_err(|e| {
                Error::ReadableTableError(format!(
                    "file path: {}, error: {}",
                    file.get_ref().get_file_path(),
                    e
                ))
            })?;

        // Build the readable block.
        let block_reader = BlockReader {
            table_context,
            decompress_handler: DecompressHandler::new(compress_type),
        };

        Ok(ReadableTable {
            fid,
            file,
            file_pool,
            closed: false,
            block_reader,
            table_meta_block: Some(table_meta_block),
            block_size,
            cache_handler,
        })
    }

    /// Build the map of meta list, add one block for each default size.
    pub fn build_index(
        block_size: u32,
        block_meta_list: &[BlockMeta],
        block_meta_list_map: &mut FxHashMap<u32, BlockMeta>,
    ) {
        let mut block_index: u32 = 0;

        for item in block_meta_list.iter() {
            block_meta_list_map.insert(block_index, item.clone());
            block_index += get_upper_bound(block_size, item.uncompressed_length) / block_size;
        }
    }

    /// Get data from cur readable table.
    /// 1. Get offset meta of this block.
    /// 2. Read, access and decompress all data of the block.
    /// 3. Get target from the collected data above.
    pub fn get(&mut self, table_offset: u64) -> Result<Vec<Vec<u8>>> {
        let table_context = &self.block_reader.table_context;

        let block_size = self.block_size;

        // Get block index.
        let block_index = ReadableTable::get_cur_block_index(table_offset, block_size);

        let start_offset = (table_offset % block_size as u64) as usize;

        let key = convert_two_u32_to_u64!(table_context.fid, block_index);

        // Get offset meta, physical offset and compress length of this block.
        let block_meta = self.get_cur_block_meta(&block_index).map_err(|e| {
            Error::ReadableTableError(format!(
                "file path: {}, table offset: {}, error: {}",
                self.get_file_path(),
                table_offset,
                e
            ))
        })?;

        let table_physical_offset = block_meta.table_offset;
        let compressed_length = block_meta.compressed_length;
        let uncompressed_length = block_meta.uncompressed_length;

        let mut block_init_parameter = BlockInitParameter {
            table_physical_offset,
            compressed_length,
            uncompressed_length,
            file: &mut self.file,
            decompress_handler: &mut self.block_reader.decompress_handler,
        };

        // Read the datablock from blockcache or disk
        let data = match self.cache_handler.block_cache.capacity() > 0 {
            true => {
                self.cache_handler
                    .block_cache
                    .get_with(&key, &mut block_init_parameter)?
                    .get(start_offset)
                    .1
            }
            false => {
                BlockCache::get_block_buffer(&mut block_init_parameter)?
                    .get(start_offset)
                    .1
            }
        };

        Ok(data)
    }

    /// Get the target block without cache.
    fn get_block_buffer(
        &mut self,
        table_physical_offset: u64,
        compressed_length: u32,
        uncompressed_length: u32,
    ) -> Result<Block> {
        // A block with compressed length buffer filled default data, which can be used
        // to read data of exact length.
        let mut compressed_buffer = vec![0u8; compressed_length as usize];

        // A block with uncompressed length buffer, which can engine the data
        // decompressed by the compressed block.
        let mut raw_buffer = vec![0u8; uncompressed_length as usize];

        // Read all the data to the compressed block.
        self.file.get_ref().read(
            &mut compressed_buffer,
            table_physical_offset,
            compressed_length,
        )?;

        // Decompress the data.
        decompress(
            &mut self.block_reader.decompress_handler,
            &mut raw_buffer,
            &compressed_buffer,
            compressed_length,
        );

        Ok(Block::new(raw_buffer))
    }

    /// Get the offset meta of cur block.
    pub fn get_cur_block_meta(&self, block_index: &u32) -> Result<&BlockMeta> {
        self.table_meta_block()
            .meta_list_map
            .get(block_index)
            .ok_or_else(|| {
                Error::ReadableTableError(format!("block index {} is null", block_index))
            })
    }

    /// Get the index of cur block on table.
    pub fn get_cur_block_index(table_offset: u64, block_size: u32) -> u32 {
        convert_u64_to_u32!(table_offset / block_size as u64)
    }

    /// Close file and drop mmap handler.
    pub fn close(&mut self) {
        if !self.closed {
            match self.cache_handler.file_cache.capacity() > 0 {
                true => {
                    if let Some(file) = self.file.take() {
                        if self.file_pool.size()
                            < self.cache_handler.file_cache.max_same_handle_num()
                        {
                            self.file_pool.push(file);
                        }
                    }
                }

                false => {
                    self.file.take();
                }
            };

            self.closed = true;
        }
    }

    pub fn get_fid(&self) -> u32 {
        self.fid
    }

    pub fn table_meta_block(&self) -> &Arc<TableMetaBlock> {
        self.table_meta_block.as_ref().unwrap()
    }

    pub fn get_file_path(&self) -> String {
        self.file.get_ref().get_file_path()
    }
}

impl<'a> BlockReader<'a> {
    /// Get Property Vector from cur readable block.
    pub fn get(cursor: usize, raw_buffer: &[u8]) -> (u32, Vec<Vec<u8>>) {
        let mut cursor = cursor;

        // Get the num of single record of the data.
        let data_num: usize = get_u32_from_bytes!(raw_buffer, cursor) as usize;

        cursor += BLOCK_RECORD_NUM_OFFSET_USIZE;

        let mut vec_record: Vec<Vec<u8>> = Vec::with_capacity(data_num);

        for _i in 0..data_num {
            let data_len: usize = get_u32_from_bytes!(raw_buffer, cursor) as usize;

            cursor += BLOCK_RECORD_LENGTH_OFFSET_USIZE;
            vec_record.push(Vec::from(&raw_buffer[cursor..cursor + data_len]));
            cursor += data_len;
        }

        (convert_usize_to_u32!(cursor), vec_record)
    }
}

/// Iterator for reading table Sequentially.
pub struct SequentialReadIter<'a> {
    // Reference of the readable table to be read.
    readable_table: ReadableTable<'a>,

    // Cache cur block which is used to get data iteratively.
    cur_block: Block,

    // Record the offset to be read on cur block.
    cur_block_offset: u32,

    // All the block meta of this readable table.
    vec_block_meta: Vec<BlockMeta>,

    // Record the block index being read on the table.
    cur_block_index: usize,
}

impl<'a> SequentialReadIter<'a> {
    pub fn new(readable_table: ReadableTable<'a>) -> Self {
        let vec_block_meta = readable_table
            .table_meta_block()
            .table_meta
            .block_meta
            .clone();

        let mut seq_read_iter = SequentialReadIter {
            readable_table: (readable_table),
            cur_block: (Block::new(vec![])),
            cur_block_offset: (0),
            vec_block_meta: (vec_block_meta),
            cur_block_index: (0),
        };

        let cur_block_meta = seq_read_iter.vec_block_meta.first().unwrap();

        seq_read_iter.cur_block = seq_read_iter
            .readable_table
            .get_block_buffer(
                cur_block_meta.table_offset,
                cur_block_meta.compressed_length,
                cur_block_meta.uncompressed_length,
            )
            .unwrap_or_else(|e| {
                error_and_panic!("{}", e);
            });

        seq_read_iter
    }
}

impl<'a> Iterator for SequentialReadIter<'a> {
    type Item = Vec<Vec<u8>>;

    /// Get one property for each iteration.
    fn next(&mut self) -> Option<Self::Item> {
        let cur_block_meta = self.vec_block_meta.get(self.cur_block_index).unwrap();

        if self.cur_block_offset == cur_block_meta.uncompressed_length {
            self.cur_block_index += 1;
            self.cur_block_offset = 0;

            if self.cur_block_index == self.vec_block_meta.len() {
                self.readable_table.close();
                return None;
            }

            let cur_block_meta = self.vec_block_meta.get(self.cur_block_index).unwrap();

            self.cur_block = self
                .readable_table
                .get_block_buffer(
                    cur_block_meta.table_offset,
                    cur_block_meta.compressed_length,
                    cur_block_meta.uncompressed_length,
                )
                .unwrap_or_else(|e| {
                    error_and_panic!("{}", e);
                });
        }

        let res = BlockReader::get(
            self.cur_block_offset as usize,
            self.cur_block.get_read_buffer(),
        );

        self.cur_block_offset = res.0;

        Some(res.1)
    }
}
