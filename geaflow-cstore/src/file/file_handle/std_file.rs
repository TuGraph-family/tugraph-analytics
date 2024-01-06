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
    fs::File,
    io::{Read, Seek, SeekFrom, Write},
    path::Path,
};

use super::{FileHandle, FileMetaData};
use crate::{error::Error, Result};

#[derive(Debug, Default)]
pub struct StdFile {
    file: Option<File>,

    // Record the file path, which is used to output to the error log when an error is reported.
    file_path: String,
}

impl StdFile {
    pub fn new(file: File, file_path: &Path) -> Self {
        Self {
            file: Some(file),
            file_path: file_path.to_str().unwrap().to_string(),
        }
    }

    fn check_file_is_some(&self) -> Result<()> {
        if self.file.is_some() {
            Ok(())
        } else {
            Err(Error::StdFileError(
                self.get_file_path(),
                "file handle is null".to_string(),
            ))
        }
    }
}

impl FileHandle for StdFile {
    fn metadata(&self) -> Result<FileMetaData> {
        self.check_file_is_some()?;

        Ok(FileMetaData {
            file_len: self
                .file
                .as_ref()
                .unwrap()
                .metadata()
                .map_err(|e| Error::StdFileError(self.file_path.clone(), e.to_string()))?
                .len(),
        })
    }

    fn read(&mut self, buf: &mut Vec<u8>, offset: u64, length: u32) -> Result<()> {
        self.check_file_is_some()?;
        let file = self.file.as_mut().unwrap();

        file.seek(SeekFrom::Start(offset))
            .map_err(|e| Error::StdFileError(self.file_path.clone(), e.to_string()))?;

        file.read_exact(buf[0..length as usize].as_mut())
            .map_err(|e| Error::StdFileError(self.file_path.clone(), e.to_string()))
    }

    fn write(&mut self, buf: &[u8]) -> Result<()> {
        self.check_file_is_some()?;
        self.file
            .as_mut()
            .unwrap()
            .write_all(buf)
            .map_err(|e| Error::StdFileError(self.file_path.clone(), e.to_string()))
    }

    fn flush(&mut self) -> Result<()> {
        self.file
            .as_mut()
            .unwrap()
            .flush()
            .map_err(|e| Error::StdFileError(self.file_path.clone(), e.to_string()))
    }

    fn get_file_path(&self) -> String {
        self.file_path.clone()
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use rand::{RngCore, SeedableRng};
    use rand_isaac::IsaacRng;

    use super::*;
    use crate::{
        file::{
            file_handle::{FileHandleMode, FileHandleType, OpenMode},
            file_operator::{FileOperator, LocalFileOperator},
        },
        test_util::bytes_are_equal,
    };

    const TEST_STD_DIR_PATH: &str = "/tmp/geaflow_cstore_local/test_std_file";
    const TEST_FILE_NAME: &str = "file_handle.test";
    const TEST_DATA_LENGTH: usize = 16 * 1024 * 1024;

    #[test]
    fn test_std_file() {
        let dir_path = Path::new(TEST_STD_DIR_PATH);
        let file_path = PathBuf::new().join(TEST_STD_DIR_PATH).join(TEST_FILE_NAME);

        LocalFileOperator.create_file(&file_path).unwrap();

        let mut write_buffer: Vec<u8> = vec![0u8; TEST_DATA_LENGTH];
        let mut rng: IsaacRng = SeedableRng::from_entropy();
        rng.fill_bytes(&mut write_buffer);

        let mut file = LocalFileOperator
            .open(
                &FileHandleType::Std(FileHandleMode::Local),
                &file_path,
                OpenMode::WriteOnly,
            )
            .unwrap();
        file.write(&write_buffer).unwrap();
        file.flush().unwrap();

        let mut read_buffer: Vec<u8> = vec![0u8; TEST_DATA_LENGTH];
        file.read(&mut read_buffer, 0, TEST_DATA_LENGTH as u32)
            .unwrap();

        assert!(bytes_are_equal(&write_buffer, &read_buffer));

        LocalFileOperator.remove_path(dir_path).unwrap();
    }
}
