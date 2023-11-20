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
    path::{Path, PathBuf},
    sync::Arc,
};

use rustc_hash::FxHashSet;
use strum_macros::{Display, EnumString};

use super::{file_handle::FileHandleType, file_operator::CstoreFileOperator};
use crate::{
    convert_u64_to_u32, error_and_panic, file::file_handle::OpenMode, log_util::info,
    manifest::CopyFileMeta, util::thread_util::RayonThreadPool, Result,
};

#[derive(Debug, Default)]
pub struct FileCopyHandler {
    inner_buffer_size: usize,
    file_operator: Arc<CstoreFileOperator>,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, EnumString, Display)]
pub enum CopyFileType {
    #[default]
    UnDefined = 0,
    VersionControlArchive,
    VersionControlRecover,
}

impl FileCopyHandler {
    pub fn new(inner_buffer_size: usize, file_operator: Arc<CstoreFileOperator>) -> Self {
        Self {
            inner_buffer_size,
            file_operator,
        }
    }

    // Copy files in multi-thread. Support source and dest file handle with
    // different type.
    pub fn copy_files(
        &self,
        thread_pool: &RayonThreadPool,
        upload_info: &FxHashSet<CopyFileMeta>,
        local_name_space: &str,
        persistent_name_space: &str,
        copy_file_type: CopyFileType,
    ) {
        thread_pool.iter_execute(upload_info, |copy_file_meta| {
            self.copy_file(copy_file_meta, local_name_space, persistent_name_space)
                .unwrap_or_else(|e| {
                    error_and_panic!("file copy type: {}, {}", copy_file_type, e);
                });
        });
    }

    // Copy single file in single thread.
    fn copy_file(
        &self,
        copy_file_meta: &CopyFileMeta,
        local_name_space: &str,
        persistent_name_space: &str,
    ) -> Result<()> {
        let src_file_handle_type = &copy_file_meta.src_file_handle_type;
        let dest_file_handle_type = &copy_file_meta.dest_file_handle_type;

        let file_name = &copy_file_meta.file_name;

        let src_name_space = if src_file_handle_type.is_local_mode() {
            local_name_space
        } else {
            persistent_name_space
        };

        let dest_name_space = if dest_file_handle_type.is_local_mode() {
            local_name_space
        } else {
            persistent_name_space
        };

        let src_path = PathBuf::new().join(src_name_space).join(file_name);
        let dest_path = PathBuf::new().join(dest_name_space).join(file_name);

        self.copy_single_file(
            src_file_handle_type,
            dest_file_handle_type,
            &src_path,
            &dest_path,
        )?;

        Ok(())
    }

    // Download single manifest in single thread.
    pub fn copy_single_file(
        &self,
        src_file_handle_type: &FileHandleType,
        dest_file_handle_type: &FileHandleType,
        src_path: &Path,
        dest_path: &Path,
    ) -> Result<()> {
        if !(src_file_handle_type.is_local_mode() ^ dest_file_handle_type.is_local_mode()) {
            info!("no need to copy files {}", src_path.display(),);
            return Ok(());
        }

        let inner_buffer_size = self.inner_buffer_size;

        // Open the source file.
        let mut src_file =
            self.file_operator
                .open(src_file_handle_type, src_path, OpenMode::ReadOnly)?;

        // Remove the dest file, create and open a new.
        self.file_operator
            .remove_file(dest_file_handle_type, dest_path)?;
        self.file_operator
            .create_file(dest_file_handle_type, dest_path)?;
        let mut dest_file =
            self.file_operator
                .open(dest_file_handle_type, dest_path, OpenMode::WriteOnly)?;

        let meta_data = src_file.metadata()?;
        let file_len = meta_data.file_len;

        let mut offset = 0;
        let mut buffer = vec![0; inner_buffer_size];

        loop {
            if (offset + inner_buffer_size as u64) > file_len {
                break;
            }
            src_file.read(&mut buffer, offset, inner_buffer_size as u32)?;
            dest_file.write(&buffer)?;

            offset += inner_buffer_size as u64;
        }

        let last_read_len = convert_u64_to_u32!(file_len % inner_buffer_size as u64);

        buffer.truncate(last_read_len as usize);

        if last_read_len != 0 {
            src_file.read(&mut buffer, offset, last_read_len)?;

            dest_file.write(&buffer[..last_read_len as usize])?;
        }

        // TODO add Debug log switch.
        info!(
            "finish copy file from {} to {}",
            src_path.to_str().unwrap(),
            dest_path.to_str().unwrap()
        );

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;
    use crate::{
        file::{
            file_handle::FileHandleMode,
            file_operator::{FileOperator, LocalFileOperator},
        },
        log_util::{self, LogLevel, LogType},
        test_util::{files_are_equal, gen_random_data},
    };

    const GEN_DATA_SIZE: usize = 10000;
    const GEN_FILE_SIZE: usize = 100;
    const TEST_NAME_SPACE: &str = "/tmp/geaflow_cstore_local/copy_files_test/";
    const SRC_NAME_SPACE: &str = "/tmp/geaflow_cstore_local/copy_files_test/src/";
    const DEST_NAME_SPACE: &str = "/tmp/geaflow_cstore_local/copy_files_test/dest/";
    const INNER_BUFFER_SIZE: usize = 1024;
    const WORK_THREADS_NUM: usize = 16;

    fn get_copy_path(
        src_name_space: &str,
        dest_name_space: &str,
        file_name: &str,
    ) -> (PathBuf, PathBuf) {
        (
            PathBuf::new().join(src_name_space).join(file_name),
            PathBuf::new().join(dest_name_space).join(file_name),
        )
    }

    #[ignore = "Timing task execute it"]
    #[test]
    fn test_copy_file() {
        log_util::try_init(LogType::ConsoleAndFile, LogLevel::Debug, 0);

        let mut copy_info: FxHashSet<CopyFileMeta> = FxHashSet::default();
        let mut copy_path_pair_set: FxHashSet<(PathBuf, PathBuf)> = FxHashSet::default();

        let file_operator = Arc::new(CstoreFileOperator::default());

        // Gen test file.
        for i in 0..GEN_FILE_SIZE {
            let file_name = i.to_string() + ".test";
            let path = PathBuf::new().join(SRC_NAME_SPACE).join(&file_name);
            LocalFileOperator.create_file(&path).unwrap();

            let mut file = LocalFileOperator
                .open(&FileHandleType::default(), &path, OpenMode::WriteOnly)
                .unwrap();

            let mut buffer = vec![0u8; GEN_DATA_SIZE];
            gen_random_data(&mut buffer);

            file.write(&buffer).unwrap();

            copy_path_pair_set.insert(get_copy_path(SRC_NAME_SPACE, DEST_NAME_SPACE, &file_name));

            copy_info.insert(CopyFileMeta {
                file_name,
                src_file_handle_type: FileHandleType::default(),
                dest_file_handle_type: FileHandleType::Std(FileHandleMode::Remote),
            });
        }

        // Test Copy files from src namespace to dest.

        let file_copy_handle = FileCopyHandler::new(INNER_BUFFER_SIZE, Arc::clone(&file_operator));
        let thread_pool = RayonThreadPool::new(WORK_THREADS_NUM);

        file_copy_handle.copy_files(
            &thread_pool,
            &copy_info,
            SRC_NAME_SPACE,
            DEST_NAME_SPACE,
            CopyFileType::UnDefined,
        );

        // Check the copy success.
        for path_pair in copy_path_pair_set {
            assert!(files_are_equal(&path_pair.0, &path_pair.1).unwrap());
        }

        LocalFileOperator
            .remove_path(&PathBuf::new().join(TEST_NAME_SPACE))
            .unwrap_or_default();
    }
}
