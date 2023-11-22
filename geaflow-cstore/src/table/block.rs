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

use crate::{
    convert_usize_to_u32, get_u32_from_bytes, BLOCK_RECORD_LENGTH_OFFSET_USIZE,
    BLOCK_RECORD_NUM_OFFSET_USIZE,
};

/// Readable block, used as the storage unit of blockcache.
#[derive(Default, Debug)]
pub struct Block {
    buffer: Vec<u8>,
}

impl Block {
    pub fn new(buffer: Vec<u8>) -> Self {
        Self { buffer }
    }

    pub fn get_read_buffer(&self) -> &[u8] {
        &self.buffer
    }

    pub fn get(&self, cursor: usize) -> (u32, Vec<Vec<u8>>) {
        let mut cursor = cursor;

        // Get the num of single record of the data.
        let data_num: usize = get_u32_from_bytes!(self.buffer, cursor) as usize;

        cursor += BLOCK_RECORD_NUM_OFFSET_USIZE;

        let mut vec_record: Vec<Vec<u8>> = Vec::with_capacity(data_num);

        for _i in 0..data_num {
            let data_len: usize = get_u32_from_bytes!(self.buffer, cursor) as usize;

            cursor += BLOCK_RECORD_LENGTH_OFFSET_USIZE;
            vec_record.push(Vec::from(&self.buffer[cursor..cursor + data_len]));
            cursor += data_len;
        }

        (convert_usize_to_u32!(cursor), vec_record)
    }
}
