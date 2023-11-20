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

use std::sync::Arc;

use opendal::BlockingOperator;

use crate::{
    file::file_handle::{FileHandle, FileMetaData},
    Result,
};

#[derive(Debug)]
pub struct OpenDalFile {
    file_path: String,
    operator: Arc<BlockingOperator>,
}

impl OpenDalFile {
    pub fn new(operator: Arc<BlockingOperator>, path: &str) -> Self {
        Self {
            operator,
            file_path: String::from(path),
        }
    }
}

impl FileHandle for OpenDalFile {
    fn metadata(&self) -> Result<FileMetaData> {
        let metadata = self.operator.stat(self.file_path.as_str())?;

        Ok(FileMetaData {
            file_len: metadata.content_length(),
        })
    }

    fn read(&mut self, buf: &mut Vec<u8>, offset: u64, length: u32) -> Result<()> {
        let end_offset = offset + length as u64;

        let tmp_buf = self
            .operator
            .read_with(self.file_path.as_str())
            .range(offset..end_offset)
            .call()?;
        buf.copy_from_slice(tmp_buf.as_slice());

        Ok(())
    }

    fn write(&mut self, buf: &[u8]) -> Result<()> {
        self.operator
            .write_with(self.file_path.as_str(), buf.to_owned())
            .append(true)
            .call()?;

        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        Ok(())
    }

    fn get_file_path(&self) -> String {
        self.file_path.to_string()
    }
}
