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

use std::path::Path;

pub use local_file_operator::LocalFileOperator;
use rustc_hash::FxHashSet;

use super::file_handle::{FileHandle, FileHandleType, OpenMode};
use crate::{
    config::{PersistentConfig, PersistentType},
    file::file_operator::opendal_file_operator::OpenDalFileOperator,
    CStoreConfig, Result,
};

mod local_file_operator;
#[cfg(feature = "opendal")]
mod opendal_file_operator;

pub trait FileOperator: Send + Sync + std::fmt::Debug {
    fn open(
        &self,
        file_handle_type: &FileHandleType,
        file_path: &Path,
        open_mode: OpenMode,
    ) -> Result<Box<dyn FileHandle>>;

    fn create_file(&self, file_path: &Path) -> Result<()>;

    fn create_dir(&self, dir_path: &Path) -> Result<()>;

    fn remove_path(&self, path: &Path) -> Result<()>;

    fn remove_file(&self, file: &Path) -> Result<()>;

    fn rename_dir(&self, src: &Path, dst: &Path) -> Result<()>;

    fn list(&self, dir_path: &Path, recursively: bool) -> Result<FxHashSet<String>>;
}

#[derive(Debug)]
pub struct CstoreFileOperator {
    local_file_operator: LocalFileOperator,
    persistent_file_operator: Box<dyn FileOperator>,
}

impl Default for CstoreFileOperator {
    fn default() -> Self {
        let persistent_config = &CStoreConfig::default().persistent;
        Self::new(persistent_config).unwrap()
    }
}

impl CstoreFileOperator {
    pub fn new(persist_config: &PersistentConfig) -> Result<Self> {
        Ok(Self {
            local_file_operator: LocalFileOperator,
            persistent_file_operator: Self::get_inner_operator(persist_config)?,
        })
    }

    fn get_inner_operator(persist_config: &PersistentConfig) -> Result<Box<dyn FileOperator>> {
        let persistent_type = &persist_config.persistent_type;

        match persistent_type {
            PersistentType::Local => Ok(Box::new(LocalFileOperator)),
            #[cfg(feature = "opendal")]
            PersistentType::Oss => Ok(Box::new(OpenDalFileOperator::new(persist_config)?)),
            #[cfg(feature = "hdfs")]
            PersistentType::Hdfs => Ok(Box::new(OpenDalFileOperator::new(persist_config)?)),
        }
    }

    pub fn open(
        &self,
        file_handle_type: &FileHandleType,
        file_path: &Path,
        open_mode: OpenMode,
    ) -> Result<Box<dyn FileHandle>> {
        if file_handle_type.is_local_handle() {
            self.local_file_operator
                .open(file_handle_type, file_path, open_mode)
        } else {
            self.persistent_file_operator
                .open(file_handle_type, file_path, open_mode)
        }
    }

    pub fn create_file(&self, file_handle_type: &FileHandleType, file_path: &Path) -> Result<()> {
        if file_handle_type.is_local_handle() {
            self.local_file_operator.create_file(file_path)
        } else {
            self.persistent_file_operator.create_file(file_path)
        }
    }

    pub fn create_dir(&self, file_handle_type: &FileHandleType, dir_path: &Path) -> Result<()> {
        if file_handle_type.is_local_handle() {
            self.local_file_operator.create_dir(dir_path)
        } else {
            self.persistent_file_operator.create_dir(dir_path)
        }
    }

    pub fn remove_path(&self, file_handle_type: &FileHandleType, path: &Path) -> Result<()> {
        if file_handle_type.is_local_handle() {
            self.local_file_operator.remove_path(path)
        } else {
            self.persistent_file_operator.remove_path(path)
        }
    }

    pub fn remove_file(&self, file_handle_type: &FileHandleType, path: &Path) -> Result<()> {
        if file_handle_type.is_local_handle() {
            self.local_file_operator.remove_file(path)
        } else {
            self.persistent_file_operator.remove_file(path)
        }
    }

    pub fn rename_dir(
        &self,
        file_handle_type: &FileHandleType,
        src: &Path,
        dst: &Path,
    ) -> Result<()> {
        if file_handle_type.is_local_handle() {
            self.local_file_operator.rename_dir(src, dst)
        } else {
            self.persistent_file_operator.rename_dir(src, dst)
        }
    }

    pub fn list(
        &self,
        file_handle_type: &FileHandleType,
        dir_path: &Path,
        recursively: bool,
    ) -> Result<FxHashSet<String>> {
        if file_handle_type.is_local_handle() {
            self.local_file_operator.list(dir_path, recursively)
        } else {
            self.persistent_file_operator.list(dir_path, recursively)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{path::PathBuf, sync::Arc};

    use super::*;
    use crate::log_util::{self, info, LogLevel, LogType};

    const TEST_LIST_FILE_RELATIVE_PATH_DIR_PATH: &str =
        "/tmp/geaflow_cstore_remote/test_list_relative";
    const TEST_LIST_DIR_PATH: &str = "/tmp/geaflow_cstore_remote/test_list";
    const TEST_CREATE_FILES_NUM: usize = 5;

    const TEST_FILE_EXTENSION: &str = "test";

    fn test_list(file_operator: &CstoreFileOperator, file_handle_type: &FileHandleType) {
        log_util::try_init(LogType::ConsoleAndFile, LogLevel::Debug, 0);

        let dir_path = Path::new(TEST_LIST_DIR_PATH);

        file_operator
            .remove_path(file_handle_type, dir_path)
            .unwrap();

        let files_name_vec = file_operator
            .list(file_handle_type, dir_path, false)
            .unwrap();
        assert_eq!(files_name_vec.len(), 0);

        for i in 0..TEST_CREATE_FILES_NUM {
            let file_path = PathBuf::new()
                .join(TEST_LIST_DIR_PATH)
                .join(i.to_string())
                .with_extension(TEST_FILE_EXTENSION);
            file_operator
                .create_file(file_handle_type, &file_path)
                .unwrap();
        }

        let files_name_vec = file_operator
            .list(file_handle_type, dir_path, false)
            .unwrap();

        assert_eq!(files_name_vec.len(), TEST_CREATE_FILES_NUM);

        file_operator
            .remove_path(file_handle_type, dir_path)
            .unwrap();
    }

    fn test_list_relatively(file_operator: &CstoreFileOperator, file_handle_type: &FileHandleType) {
        log_util::try_init(LogType::ConsoleAndFile, LogLevel::Debug, 0);

        let dir_path = Path::new(TEST_LIST_FILE_RELATIVE_PATH_DIR_PATH);

        file_operator
            .remove_path(file_handle_type, dir_path)
            .unwrap();

        for i in 0..TEST_CREATE_FILES_NUM {
            let file_path = PathBuf::new()
                .join(TEST_LIST_FILE_RELATIVE_PATH_DIR_PATH)
                .join(i.to_string())
                .with_extension(TEST_FILE_EXTENSION);
            file_operator
                .create_file(file_handle_type, &file_path)
                .unwrap();
        }

        let child1_dir_path = PathBuf::new()
            .join(TEST_LIST_FILE_RELATIVE_PATH_DIR_PATH)
            .join("test1");

        for i in 0..TEST_CREATE_FILES_NUM {
            let file_path = PathBuf::new()
                .join(&child1_dir_path)
                .join(i.to_string())
                .with_extension(TEST_FILE_EXTENSION);
            file_operator
                .create_file(file_handle_type, &file_path)
                .unwrap();
        }

        let child2_dir_path = PathBuf::new()
            .join(TEST_LIST_FILE_RELATIVE_PATH_DIR_PATH)
            .join("test1")
            .join("test2");

        for i in 0..TEST_CREATE_FILES_NUM {
            let file_path = PathBuf::new()
                .join(&child2_dir_path)
                .join(i.to_string())
                .with_extension(TEST_FILE_EXTENSION);
            file_operator
                .create_file(file_handle_type, &file_path)
                .unwrap();
        }

        let files_name_vec = file_operator
            .list(file_handle_type, dir_path, true)
            .unwrap();

        info!("list_file_relatively: {:?}", files_name_vec);

        assert_eq!(files_name_vec.len(), 3 * TEST_CREATE_FILES_NUM);

        file_operator
            .remove_path(file_handle_type, dir_path)
            .unwrap();
    }

    #[test]
    fn test_list_function() {
        let file_operator = Arc::new(CstoreFileOperator::default());

        test_list(&file_operator, &FileHandleType::default());
        test_list_relatively(&file_operator, &FileHandleType::default());
    }
}
