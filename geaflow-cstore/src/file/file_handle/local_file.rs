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

use std::{
    fs::{create_dir_all, File},
    io::{BufReader, BufWriter, Read, Write},
    path::Path,
};

use crate::{
    error::Error,
    file::file_operator::{FileOperator, LocalFileOperator},
    util::string_util::get_rfind_front_section,
    Result, FILE_PATH_DELIMITER,
};

pub struct LocalBufferWriter {
    writer: BufWriter<File>,
    file_path: String,
}

impl LocalBufferWriter {
    pub fn new(file_path: &Path) -> Result<Self> {
        let file_path_str = file_path.to_str().unwrap().to_string();
        let path = get_rfind_front_section(file_path.to_str().unwrap(), FILE_PATH_DELIMITER);

        create_dir_all(path)
            .map_err(|e| Error::LocalBufferWriter(file_path_str.clone(), e.to_string()))?;

        LocalFileOperator
            .remove_file(file_path)
            .map_err(|e| Error::LocalBufferWriter(file_path_str.clone(), e.to_string()))?;

        let file = File::create(file_path)
            .map_err(|e| Error::LocalBufferWriter(file_path_str.clone(), e.to_string()))?;

        let writer = BufWriter::new(file);

        Ok(Self {
            writer,
            file_path: file_path_str,
        })
    }

    pub fn write(&mut self, buf: &[u8]) -> Result<()> {
        self.writer
            .write_all(buf)
            .map_err(|e| Error::LocalBufferWriter(self.file_path.clone(), e.to_string()))
    }

    pub fn flush(&mut self) -> Result<()> {
        self.writer
            .flush()
            .map_err(|e| Error::LocalBufferWriter(self.file_path.clone(), e.to_string()))
    }
}

pub struct LocalBufferReader {
    reader: BufReader<File>,
    file_path: String,
    file_len: u64,
}

impl LocalBufferReader {
    pub fn new(file_path: &Path) -> Result<Self> {
        let file_path_str = file_path.to_str().unwrap().to_string();

        let path = get_rfind_front_section(file_path.to_str().unwrap(), FILE_PATH_DELIMITER);

        create_dir_all(path)
            .map_err(|e| Error::LocalBufferReader(file_path_str.clone(), e.to_string()))?;

        let file = File::open(file_path)
            .map_err(|e| Error::LocalBufferReader(file_path_str.clone(), e.to_string()))?;

        let file_len = file.metadata().unwrap().len();
        let reader = BufReader::new(file);
        Ok(Self {
            reader,
            file_path: file_path_str,
            file_len,
        })
    }

    pub fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        let usize = self
            .reader
            .read(buf)
            .map_err(|e| Error::LocalBufferReader(self.file_path.clone(), e.to_string()))?;

        Ok(usize)
    }

    pub fn get_file_len(&self) -> u64 {
        self.file_len
    }
}
