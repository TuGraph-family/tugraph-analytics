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

#![allow(non_upper_case_globals)]

use std::{fmt::Display, str::FromStr};

pub use block::Block;
use num_enum::FromPrimitive;
pub use readable_table::{ReadableTable, SequentialReadIter};
use strum_macros::{Display, EnumString};
pub use writable_table::WritableTable;

use crate::{
    config::BaseConfig,
    file::file_handle::FileHandleType,
    util::string_util::{get_rfind_back_section, get_rfind_front_section},
    Result, DATA_AND_INDEX_FILE_NAME_DELIMITER, MANIFEST_FILE_NAME_DELIMITER,
    MANIFEST_FILE_NAME_PREFIX,
};

/// Readable tables, including the definition of readable tables and the
/// interface for reading functions.
mod readable_table;

/// Writable tables, including the definition of writable tables and the
/// interface for writing functions.
mod writable_table;

/// Block for data writing and reading.
mod block;

#[derive(Clone, Copy, EnumString, Display, PartialEq, FromPrimitive, Default)]
#[repr(u32)]
#[strum(ascii_case_insensitive)]
pub enum TableType {
    #[default]
    Is = 1, //  index segment

    Vs, //  value segment

    Mi, //  memory index segment

    Ms, //  manifest
}

impl TableType {
    pub fn is_index_or_data_table(&self) -> bool {
        *self == TableType::Is || *self == TableType::Vs
    }

    pub fn is_index_table(&self) -> bool {
        *self == TableType::Is
    }
}

/// Get the up bound offset of table.
#[inline]
pub fn get_upper_bound(block_size: u32, offset: u32) -> u32 {
    let block_offset = offset % block_size;
    if block_offset == 0 {
        offset
    } else {
        offset - block_offset + block_size
    }
}

pub fn get_table_name_space<'a>(
    file_handle_type: &FileHandleType,
    base: &'a BaseConfig,
) -> &'a str {
    if file_handle_type.is_local_mode() {
        &base.local_name_space
    } else {
        &base.persistent_name_space
    }
}

pub fn build_file_name<T>(id: T, table_type: TableType) -> String
where
    T: Display,
{
    match &table_type {
        // Id is version in manifest file.
        TableType::Ms => format!("{}-{}", MANIFEST_FILE_NAME_PREFIX, id),
        // Id is Fid in index file and data file.
        _ => format!("{}.{}", id, table_type.to_string().to_ascii_lowercase()),
    }
}

pub fn get_table_type(file_name: &str) -> Result<TableType> {
    let suffix = get_rfind_back_section(file_name, DATA_AND_INDEX_FILE_NAME_DELIMITER);
    let table_type = if suffix != file_name {
        TableType::from_str(suffix)?
    } else {
        let prefix = get_rfind_front_section(file_name, MANIFEST_FILE_NAME_DELIMITER);
        TableType::from_str(prefix)?
    };

    Ok(table_type)
}
