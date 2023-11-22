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
    config::TableConfig,
    file::{file_handle::FileHandleType, file_operator::CstoreFileOperator},
    util::compress_util::CompressType,
    TableType,
};

/// Table context, including table type, compression type and file id.
pub struct TableContext<'a> {
    pub table_type: TableType,

    pub compress_type: CompressType,

    pub file_handle_type: FileHandleType,

    pub fid: u32,

    pub file_operator: &'a CstoreFileOperator,

    pub enable_cache: bool,
}

impl<'a> TableContext<'a> {
    pub fn new(
        table_config: &TableConfig,
        fid: u32,
        file_operator: &'a CstoreFileOperator,
    ) -> TableContext<'a> {
        TableContext {
            table_type: TableType::Vs,
            compress_type: table_config.compress_type,
            file_handle_type: table_config.base.file_handle_type,
            fid,
            file_operator,
            enable_cache: true,
        }
    }

    pub fn with_enable_cache(mut self, enable_cache: bool) -> Self {
        self.enable_cache = enable_cache;
        self
    }

    pub fn with_compress_type(mut self, compress_type: CompressType) -> Self {
        self.compress_type = compress_type;
        self
    }

    pub fn with_table_type(mut self, table_type: TableType) -> Self {
        self.table_type = table_type;
        self
    }

    pub fn with_file_handle_type(mut self, file_handle_type: FileHandleType) -> Self {
        self.file_handle_type = file_handle_type;
        self
    }
}
