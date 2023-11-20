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

use std::{mem, result};

use serde::{Deserialize, Serialize};

use self::error::Error;

pub mod cache;
pub mod error;
pub mod gen;
pub mod iterator;

pub const EMPTY_SECOND_KEY_SEQUENCE_ID: u64 = 0;
pub const INITIAL_SECOND_KEY_SEQUENCE_ID: u64 = 1;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct SecondKey {
    pub ts: u32,

    // graph meta includes graph_info and target_id, it will be used to filter
    // index in advance.
    pub graph_info: u64,

    pub target_id: Vec<u8>,

    pub sequence_id: u64,
}

impl SecondKey {
    pub fn build_empty_second_key() -> Self {
        Self {
            ts: 0,
            graph_info: 0,
            target_id: vec![],
            sequence_id: EMPTY_SECOND_KEY_SEQUENCE_ID,
        }
    }

    pub fn get_second_key_size(&self) -> usize {
        SECOND_FIXED_LENGTH + self.target_id.len()
    }

    pub fn set_empty(&mut self) {
        self.sequence_id = EMPTY_SECOND_KEY_SEQUENCE_ID;
    }

    pub fn is_empty(&self) -> bool {
        self.sequence_id == EMPTY_SECOND_KEY_SEQUENCE_ID
    }
}

#[derive(Clone, Debug)]
pub struct GraphData {
    pub second_key: SecondKey,

    pub property: Vec<u8>,
}

// property address and vector of second key.
#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct GraphDataIndex {
    pub graph_data_fid: u32,

    pub graph_data_offset: u64,

    pub all_filtered: bool,

    pub second_keys: Vec<SecondKey>,
}

pub const SIZE_OF_U8: usize = mem::size_of::<u8>();
pub const SIZE_OF_U16: usize = mem::size_of::<u16>();
pub const SIZE_OF_U32: usize = mem::size_of::<u32>();
pub const SIZE_OF_U64: usize = mem::size_of::<u64>();
pub const SIZE_OF_USIZE: usize = mem::size_of::<u64>();

pub const SOURCE_ID_SIZE: usize = SIZE_OF_U32;

// Segment Begin
pub const KEY_SIZE: usize = SIZE_OF_U32;
pub const SECOND_KEY_VEC_LEN_SIZE: usize = SIZE_OF_U32;
pub const FID_SIZE: usize = SIZE_OF_U32;
pub const TABLE_OFFSET_SIZE: usize = SIZE_OF_U64;
pub const SECOND_KEY_TS_SIZE: usize = SIZE_OF_U32;
pub const SECOND_KEY_GRAPH_INFO_SIZE: usize = SIZE_OF_U64;
pub const SECOND_KEY_SEQUENCE_SIZE: usize = SIZE_OF_U64;
pub const SECOND_FIXED_LENGTH: usize =
    SECOND_KEY_TS_SIZE + SECOND_KEY_GRAPH_INFO_SIZE + SECOND_KEY_SEQUENCE_SIZE;
pub const SECOND_KEY_TARGET_LEN_SIZE: usize = SIZE_OF_U32;
pub const SERIALIZED_GRAPH_DATA_INDEX_LEN: usize = SIZE_OF_U32;
pub const INDEX_BLOCK_BINARY_HEADER: usize = SOURCE_ID_SIZE + SERIALIZED_GRAPH_DATA_INDEX_LEN;
// Segment End

// Table Begin
pub const BLOCK_RECORD_NUM_SIZE_U32: u32 = SIZE_OF_U32 as u32; // Size of block_record_num in block.
pub const BLOCK_RECORD_NUM_SIZE_U64: u64 = SIZE_OF_U32 as u64; // Size of block_record_num in file.
pub const BLOCK_RECORD_NUM_OFFSET_USIZE: usize = SIZE_OF_U32; // Sizeof(block_record_num).
pub const PROPERTY_META_LENGTH_OFFSET_USIZE: usize =
    BLOCK_RECORD_NUM_OFFSET_USIZE + BLOCK_RECORD_LENGTH_OFFSET_USIZE; // Sizeof(property_meta_length).
pub const BLOCK_RECORD_LENGTH_SIZE_U32: u32 = SIZE_OF_U32 as u32; // Size of block_record_length.
pub const BLOCK_RECORD_LENGTH_OFFSET_USIZE: usize = SIZE_OF_U32; // Sizeof(block_record_length)
pub const FILE_PATH_DELIMITER: &str = "/"; // Default delimeter of the file path.
// Table End

// Index Begin
pub const INDEX_KEY_SIZE: usize = SIZE_OF_U32;
pub const INDEX_VALUE_LEN_SIZE: usize = SIZE_OF_U32;
// Index End

// Manifest Begin
pub const MANIFEST_FILE_NAME_PREFIX: &str = "MANIFEST";
pub const MANIFEST_FILE_NAME_DELIMITER: &str = "-";
pub const DATA_AND_INDEX_FILE_NAME_DELIMITER: &str = ".";
pub const DIR_NAME_SPLICER: &str = "_";
pub const CHECK_POINT_SUFFIX: &str = "chk";
// Manifest End

pub type Result<T> = result::Result<T, Error>;
