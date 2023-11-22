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

use std::str::FromStr;

pub use local_file::{LocalBufferReader, LocalBufferWriter};
pub use mmap_file::{MmapFile, DEFAULT_EXTEND_LENGTH};
use num_enum::{FromPrimitive, IntoPrimitive};
#[cfg(feature = "opendal")]
pub use opendal_file::OpenDalFile;
pub use std_file::StdFile;
use strum_macros::{Display, EnumString};

use crate::{
    error::Error,
    util::string_util::{get_rfind_back_section, get_rfind_front_section},
    Result,
};

mod local_file;
mod mmap_file;
mod std_file;

#[cfg(feature = "opendal")]
mod opendal_file;

#[derive(Debug, Default, Clone, Copy)]
pub struct FileMetaData {
    pub file_len: u64,
}

#[derive(Clone, Copy)]
pub enum OpenMode {
    ReadOnly = 1,
    WriteOnly,
    ReadWrite,
}

#[derive(Clone, Copy, Display, EnumString, PartialEq, Default, Debug, Eq, Hash)]
#[strum(ascii_case_insensitive)]
pub enum FileHandleMode {
    #[default]
    Local,
    Remote,
}

#[derive(Clone, Copy, Debug, FromPrimitive, IntoPrimitive, Display, PartialEq)]
#[repr(u32)]
pub enum PathType {
    #[default]
    None = 0,
    File,
    Dir,
}

/// Set of the file handle type.
#[derive(Clone, Copy, PartialEq, Debug, Eq, Hash, Display)]
pub enum FileHandleType {
    Std(FileHandleMode),
    Mmap(FileHandleMode),
    #[cfg(feature = "opendal")]
    Oss,
    #[cfg(feature = "hdfs")]
    Hdfs,
}

impl FromStr for FileHandleType {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        let s = s.trim();
        let err_info = format!("invalid str {} to load FileHandleType", s);

        let mut file_mode = FileHandleMode::default();
        let mut file_handle_type_str = s.to_string().to_ascii_lowercase();

        if file_handle_type_str.contains('(') {
            let mut file_mode_str = get_rfind_back_section(s, "(");
            file_mode_str = get_rfind_front_section(file_mode_str, ")");
            file_mode = FileHandleMode::from_str(file_mode_str)?;
            file_handle_type_str = get_rfind_front_section(&file_handle_type_str, "(").to_string();
        }

        match file_handle_type_str.as_str() {
            "std" => Ok(FileHandleType::Std(file_mode)),
            "mmap" => Ok(FileHandleType::Mmap(file_mode)),
            #[cfg(feature = "opendal")]
            "oss" => Ok(FileHandleType::Oss),
            #[cfg(feature = "hdfs")]
            "hdfs" => Ok(FileHandleType::Hdfs),
            _ => Err(Error::FileHandleTypeError(err_info.clone())),
        }
    }
}

impl Default for FileHandleType {
    fn default() -> Self {
        FileHandleType::Std(FileHandleMode::default())
    }
}

impl FileHandleType {
    pub fn is_local_mode(&self) -> bool {
        matches!(
            *self,
            Self::Std(FileHandleMode::Local) | Self::Mmap(FileHandleMode::Local)
        )
    }

    pub fn is_local_handle(&self) -> bool {
        matches!(*self, Self::Std(_) | Self::Mmap(_))
    }
}

// FileHandle trait, include basic file read-write function.
pub trait FileHandle: Send + Sync {
    // Get the meta data of the file.
    fn metadata(&self) -> Result<FileMetaData>;

    // Seek the cursor at the specified location of the file and read data with the
    // same length as the incoming buffer.
    fn read(&mut self, buf: &mut Vec<u8>, offset: u64, length: u32) -> Result<()>;

    // Writes all data in the incoming buffer to the file.
    fn write(&mut self, buf: &[u8]) -> Result<()>;

    // Flush all data in the buffer to disk.
    fn flush(&mut self) -> Result<()>;

    // The file path is used to print logs. Other file metadata needs to be obtained
    // through the stat file handle.
    fn get_file_path(&self) -> String;
}
