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

#![allow(clippy::enum_variant_names)]

use std::io;

use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    // Error introduced from external crate.
    #[error("StrumParseError {0}")]
    StrumParseError(#[from] strum::ParseError),
    #[error("StripPrefixError {0}")]
    StripPrefixError(#[from] std::path::StripPrefixError),
    #[error("ProstDecodeError {0}")]
    ProstDecodeError(#[from] prost::DecodeError),
    #[error("ProstEncodeError {0}")]
    ProstEncodeError(#[from] prost::EncodeError),
    #[cfg(feature = "opendal")]
    #[error("OpenDalError {0}")]
    OpenDalError(#[from] opendal::Error),
    #[error("IoError {0}")]
    IoError(#[from] io::Error),

    // Error include archiving and recovery.
    #[error("ArchiveError {0}")]
    ArchiveError(String),
    #[error("RecoverError {0}")]
    RecoverError(String),

    // Error include file system and file handle.
    #[error("FileHandleTypeError {0}")]
    FileHandleTypeError(String),

    // Error include each file handle
    #[error("StdFileError path: {0}, {1}")]
    StdFileError(String, String),
    #[error("MmapFileError path: {0}, {1}")]
    MmapFileError(String, String),
    #[error("LocalBufferReader path: {0}, {1}")]
    LocalBufferReader(String, String),
    #[error("LocalBufferWriter path: {0}, {1}")]
    LocalBufferWriter(String, String),

    // Error include each file operator
    #[error("LocalFileOperatorError path: {0}, {1}")]
    LocalFileOperatorError(String, String),

    // Error include table.
    #[error("ReadableTableError {0}")]
    ReadableTableError(String),

    // Error include compaction.
    #[error("CompactError {0}")]
    CompactError(String),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        log_util::{self, info, LogLevel, LogType},
        Result,
    };

    fn test_io_err(err_flag: u32) -> Result<()> {
        match err_flag {
            1 => Err(Error::StdFileError(
                "tmp/test/0.test".to_string(),
                std::io::Error::from_raw_os_error(13).to_string(),
            )),
            2 => Err(Error::ArchiveError(format!("Test Archive Error!"))),
            3 => Err(Error::RecoverError(format!("Test Recover Error!"))),
            _ => Ok(()),
        }
    }

    #[test]
    fn test_error() {
        log_util::try_init(LogType::ConsoleAndFile, LogLevel::Debug, 0);
        for i in 1..3 {
            let error = test_io_err(i);

            match error {
                Err(err) => info!("{}", err),

                Ok(()) => info!("Ok!"),
            }
        }
    }
}
