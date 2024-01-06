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

#![allow(unused)]

use std::{fs::File, path::Path};

use bytes::BufMut;
use memmap2::MmapMut;

use super::{FileHandle, FileMetaData};
use crate::{convert_u64_to_usize, error::Error, Result};

/// length of extend file once
/// When the following situations occur
/// 1. file length is zero
/// 2. file length < write offset + write data length
pub const DEFAULT_EXTEND_LENGTH: u64 = 1 << 28;

/// MmapFile, it can be used to read and write file bypass page cache.
#[derive(Debug, Default)]
pub struct MmapFile {
    // access file handle
    file: Option<File>,

    // mmap handle
    mmap: Option<MmapMut>,

    // cursor to map data
    cursor: u64,

    // Record the file path, which is used to output to the error log when an error is reported.
    file_path: String,
}

impl MmapFile {
    pub fn new(file: File, mmap: MmapMut, file_path: &Path) -> Self {
        Self {
            file: Some(file),
            mmap: Some(mmap),
            cursor: 0,
            file_path: file_path.to_str().unwrap().to_string(),
        }
    }
}

impl MmapFile {
    fn extend_file_len(&mut self, length: u64) {
        if let Some(ref mut file) = self.file {
            file.set_len(length).unwrap();
        }
        self.remap();
    }

    fn get_file_len(&self) -> u64 {
        if let Some(ref mmap) = self.mmap {
            mmap.len() as u64
        } else {
            0
        }
    }

    pub fn remap(&mut self) {
        if let Some(ref file) = self.file {
            self.mmap = unsafe { Some(MmapMut::map_mut(file).unwrap()) };
        }
    }

    // Set file len. File would be truncated when entering length is shorter
    // than file length.
    pub fn set_file_len(&mut self, length: u64) {
        if let Some(ref mut file) = self.file {
            file.set_len(length).unwrap();
        }
    }

    fn check_file_is_some(&self) -> Result<()> {
        if self.file.is_some() && self.mmap.is_some() {
            Ok(())
        } else {
            Err(Error::MmapFileError(
                self.get_file_path(),
                "mmap file handle is not initialized".to_string(),
            ))
        }
    }
}

impl FileHandle for MmapFile {
    fn metadata(&self) -> Result<FileMetaData> {
        self.check_file_is_some()?;

        Ok(FileMetaData {
            file_len: self
                .file
                .as_ref()
                .unwrap()
                .metadata()
                .map_err(|e| Error::MmapFileError(self.file_path.clone(), e.to_string()))?
                .len(),
        })
    }

    fn read(&mut self, buf: &mut Vec<u8>, offset: u64, length: u32) -> Result<()> {
        self.check_file_is_some()?;

        let offset = offset as usize;
        let length: usize = length as usize;
        buf.put_slice(&self.mmap.as_mut().unwrap()[offset..offset + length]);
        Ok(())
    }

    fn write(&mut self, buf: &[u8]) -> Result<()> {
        self.check_file_is_some()?;

        let cursor = self.cursor;
        let data_length = buf.len() as u64;
        if cursor + data_length > self.get_file_len() {
            self.extend_file_len(self.cursor + std::cmp::max(data_length, DEFAULT_EXTEND_LENGTH));
        }

        // Wait for the file to finish extending length.
        while cursor + data_length > self.get_file_len() {}

        self.mmap.as_mut().unwrap()
            [convert_u64_to_usize!(cursor)..convert_u64_to_usize!(cursor + data_length)]
            .copy_from_slice(buf);

        self.cursor += data_length;

        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        self.check_file_is_some()?;

        self.mmap.as_mut().unwrap().flush().unwrap();
        Ok(())
    }

    fn get_file_path(&self) -> String {
        self.file_path.clone()
    }
}
