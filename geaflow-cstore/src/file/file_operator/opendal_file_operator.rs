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

use std::{collections::HashMap, path::Path, sync::Arc};

use opendal::{
    layers::BlockingLayer,
    raw::{build_rel_path, get_parent},
    BlockingOperator, Operator, Scheme,
};
use rustc_hash::FxHashSet;
use tokio::runtime::Runtime;

use crate::{
    config::{PersistentConfig, PersistentType},
    error::Error::OpenDalError,
    file::{
        file_handle::{FileHandle, FileHandleType, OpenDalFile, OpenMode},
        file_operator::FileOperator,
    },
    util::string_util::{get_rel_path, normalize_dir_path},
    Result, FILE_PATH_DELIMITER,
};

const ROOT: &str = "root";

#[derive(Debug)]
pub struct OpenDalFileOperator {
    operator: Arc<BlockingOperator>,
    root: String,
    _rt: Option<Runtime>,
}

impl OpenDalFileOperator {
    pub fn new(persist_config: &PersistentConfig) -> Result<Self> {
        let mut map = match &persist_config.config {
            None => HashMap::default(),
            Some(config) => config.clone(),
        };
        let mut root = String::from(&persist_config.root);
        if !persist_config.root.as_str().ends_with(FILE_PATH_DELIMITER) {
            root.push_str(FILE_PATH_DELIMITER)
        }
        map.insert(ROOT.to_string(), String::from(&root));

        #[allow(unreachable_patterns)]
        let scheme = match &persist_config.persistent_type {
            PersistentType::Local => Scheme::Fs,
            PersistentType::Oss => Scheme::Oss,
            #[cfg(feature = "hdfs")]
            PersistentType::Hdfs => Scheme::Hdfs,
            _ => unreachable!(),
        };
        let mut op = Operator::via_map(scheme, map)?;
        let mut runtime_opt: Option<Runtime> = None;

        if !op.info().full_capability().blocking {
            let rt = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()?;

            let _guard = rt.enter();
            op = op.layer(BlockingLayer::create()?);
            runtime_opt = Some(rt);
        }

        Ok(Self {
            operator: Arc::new(op.blocking()),
            root,
            _rt: runtime_opt,
        })
    }

    fn is_dir_path(&self, path: &str) -> Result<bool> {
        if path.ends_with(FILE_PATH_DELIMITER) {
            Ok(true)
        } else {
            self.is_dir_exist(path)
        }
    }

    fn is_dir_exist(&self, path: &str) -> Result<bool> {
        let dir_path = normalize_dir_path(path);
        let result = self.operator.stat(&dir_path);
        match result {
            Ok(metadata) => Ok(metadata.is_dir()),
            Err(err) => match err.kind() {
                opendal::ErrorKind::NotFound => Ok(false),
                _ => Err(OpenDalError(err)),
            },
        }
    }

    fn build_rel_path(&self, path: &Path) -> String {
        let path_str = path.to_str().unwrap();
        build_rel_path(&self.root, path_str)
    }

    fn list_recursive_files(&self, dir_path: &str, abs_path: bool) -> Result<FxHashSet<String>> {
        let mut file_names: FxHashSet<String> = FxHashSet::default();

        let entries = self.operator.list_with(dir_path).recursive(true).call()?;

        for entry in entries {
            if entry.metadata().is_file() {
                if abs_path {
                    file_names.insert(entry.path().to_string());
                } else {
                    file_names.insert(get_rel_path(dir_path, entry.path()));
                }
            }
        }

        Ok(file_names)
    }
}

impl FileOperator for OpenDalFileOperator {
    fn open(
        &self,
        _file_handle_type: &FileHandleType,
        file_path: &Path,
        _open_mode: OpenMode,
    ) -> Result<Box<dyn FileHandle>> {
        let file_path_str = self.build_rel_path(file_path);
        let handle = OpenDalFile::new(Arc::clone(&self.operator), &file_path_str);

        Ok(Box::new(handle))
    }

    fn create_file(&self, file_path: &Path) -> Result<()> {
        let file_path_str = file_path.to_str().unwrap();
        let dir_path_str = get_parent(file_path_str);
        let rel_dir_path = build_rel_path(&self.root, dir_path_str);
        let ref_file_path = build_rel_path(&self.root, file_path_str);
        self.operator.create_dir(&rel_dir_path)?;
        self.operator.delete(&ref_file_path)?;

        Ok(())
    }

    fn create_dir(&self, dir_path: &Path) -> Result<()> {
        let rel_dir_path = self.build_rel_path(dir_path);
        self.operator
            .create_dir(&normalize_dir_path(&rel_dir_path))?;

        Ok(())
    }

    fn remove_path(&self, path: &Path) -> Result<()> {
        let rel_dir_path = self.build_rel_path(path);
        if self.is_dir_path(&rel_dir_path)? {
            self.operator
                .remove_all(&normalize_dir_path(&rel_dir_path))?
        } else {
            self.operator.remove_all(&rel_dir_path)?
        }

        Ok(())
    }

    fn remove_file(&self, file_path: &Path) -> Result<()> {
        let rel_file_path = self.build_rel_path(file_path);
        self.operator.delete(&rel_file_path)?;

        Ok(())
    }

    fn rename_dir(&self, src: &Path, dst: &Path) -> Result<()> {
        let src_path = normalize_dir_path(&self.build_rel_path(src));
        let dst_path = normalize_dir_path(&self.build_rel_path(dst));

        self.operator.create_dir(&dst_path)?;
        let old_files = self.list_recursive_files(&src_path, true)?;

        if self.operator.info().full_capability().rename {
            for src_file in &old_files {
                let dst_file = src_file.replace(&src_path, &dst_path);
                self.operator.rename(src_file, &dst_file)?;
            }
        } else {
            for src_file in &old_files {
                let dst_file = src_file.replace(&src_path, &dst_path);
                self.operator.copy(src_file, &dst_file)?;
            }
        }
        self.operator.remove_all(&src_path)?;

        Ok(())
    }

    fn list(&self, dir_path: &Path, recursively: bool) -> Result<FxHashSet<String>> {
        let rel_dir_path = &normalize_dir_path(&self.build_rel_path(dir_path));

        if recursively {
            self.list_recursive_files(rel_dir_path, false)
        } else {
            let entries = self.operator.list(rel_dir_path)?;

            let mut file_names = FxHashSet::default();
            for entry in entries {
                file_names.insert(entry.name().to_string());
            }

            Ok(file_names)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use rand::{RngCore, SeedableRng};
    use rand_isaac::IsaacRng;

    use super::*;
    #[cfg(feature = "hdfs")]
    use crate::test_util::build_hdfs_persistent_config;
    use crate::{
        log_util,
        log_util::{info, LogLevel, LogType},
        test_util::{build_oss_persistent_config, bytes_are_equal},
        CStoreConfigBuilder, ConfigMap,
    };

    const TEST_DATA_LENGTH: usize = 16 * 1024 * 1024;
    const TEST_STD_DIR_PATH: &str = "/tmp/geaflow_cstore_local/test_fs_file/";
    const TEST_LIST_DIR_PATH: &str = "/tmp/geaflow_cstore_local/test_fs_list/";
    const TEST_LIST_FILE_RELATIVE_PATH_DIR_PATH: &str =
        "/tmp/geaflow_cstore_local/test_list_relative/";

    const TEST_CREATE_FILES_NUM: usize = 5;
    const TEST_FILE_EXTENSION: &str = "test";
    const TEST_FILE_NAME: &str = "file_handle.test";

    #[test]
    fn test_fs_file() {
        let dir_path = Path::new(TEST_STD_DIR_PATH);
        let file_path = dir_path.join(TEST_FILE_NAME);

        let operator = OpenDalFileOperator::new(&build_local_test_config()).unwrap();
        operator.create_file(&file_path).unwrap();

        let mut write_buffer: Vec<u8> = vec![0u8; TEST_DATA_LENGTH];
        let mut rng: IsaacRng = SeedableRng::from_entropy();
        rng.fill_bytes(&mut write_buffer);

        let mut file = operator
            .open(&FileHandleType::default(), &file_path, OpenMode::WriteOnly)
            .unwrap();
        file.write(&write_buffer).unwrap();
        file.write(&write_buffer).unwrap();
        file.flush().unwrap();

        let mut read_buffer: Vec<u8> = vec![0u8; TEST_DATA_LENGTH];
        file.read(&mut read_buffer, 0, TEST_DATA_LENGTH as u32)
            .unwrap();

        let mut read_buffer2: Vec<u8> = vec![0u8; TEST_DATA_LENGTH];
        file.read(
            &mut read_buffer2,
            TEST_DATA_LENGTH as u64,
            TEST_DATA_LENGTH as u32 * 2,
        )
        .unwrap();

        assert!(bytes_are_equal(&write_buffer, &read_buffer));
        assert!(bytes_are_equal(&write_buffer, &read_buffer2));

        operator.remove_path(dir_path).unwrap();
    }

    #[test]
    fn test_local_rename_dir() {
        let src = "/tmp/geaflow_cstore_local/test_rename_dir_1/";
        let dst = "/tmp/geaflow_cstore_local/test_rename_dir_2/";

        let operator = OpenDalFileOperator::new(&build_local_test_config()).unwrap();
        operator.remove_path(Path::new(dst)).unwrap();
        operator.create_dir(Path::new(src)).unwrap();
        create_files_and_directories(&operator, src, &FileHandleType::default());

        operator.rename_dir(Path::new(src), Path::new(dst)).unwrap();
        let src_files = operator
            .list_recursive_files("test_rename_dir_1/", true)
            .unwrap();
        assert_eq!(src_files.len(), 0);
        let dst_files = operator
            .list_recursive_files("test_rename_dir_2/", true)
            .unwrap();
        assert_eq!(dst_files.len(), 15);

        assert!(!Path::new(src).exists());
        assert!(Path::new(dst).exists());
        operator.remove_path(Path::new(dst)).unwrap();
        assert!(!Path::new(dst).exists());
    }

    #[test]
    fn test_list_function() {
        let file_operator = OpenDalFileOperator::new(&build_local_test_config()).unwrap();

        test_list(
            &file_operator,
            &FileHandleType::default(),
            TEST_LIST_DIR_PATH,
        );
        test_list_file_relative_path(
            &file_operator,
            &FileHandleType::default(),
            TEST_LIST_FILE_RELATIVE_PATH_DIR_PATH,
        );
    }

    fn test_list(
        file_operator: &OpenDalFileOperator,
        file_handle_type: &FileHandleType,
        dir_path_str: &str,
    ) {
        log_util::try_init(LogType::ConsoleAndFile, LogLevel::Debug, 0);

        let dir_path = Path::new(dir_path_str);
        file_operator.remove_path(dir_path).unwrap();

        let files_name_vec = file_operator.list(dir_path, false).unwrap();
        assert_eq!(files_name_vec.len(), 0);

        for i in 0..TEST_CREATE_FILES_NUM {
            let file_path = PathBuf::new()
                .join(dir_path_str)
                .join(i.to_string())
                .with_extension(TEST_FILE_EXTENSION);
            file_operator.create_file(&file_path).unwrap();
            let mut file = file_operator
                .open(file_handle_type, &file_path, OpenMode::WriteOnly)
                .unwrap();
            file.write(&vec![0u8; 3]).unwrap();
        }

        let files_name_vec = file_operator.list(dir_path, false).unwrap();

        assert_eq!(files_name_vec.len(), TEST_CREATE_FILES_NUM);
        file_operator.remove_path(dir_path).unwrap();
    }

    fn test_list_file_relative_path(
        file_operator: &OpenDalFileOperator,
        file_handle_type: &FileHandleType,
        dir_path_str: &str,
    ) {
        log_util::try_init(LogType::ConsoleAndFile, LogLevel::Debug, 0);

        let dir_path = Path::new(dir_path_str);

        file_operator.remove_path(dir_path).unwrap();
        create_files_and_directories(file_operator, dir_path_str, file_handle_type);

        let files_name_vec = file_operator.list(dir_path, true).unwrap();

        info!("list_file_relative_path: {:?}", files_name_vec);

        assert_eq!(files_name_vec.len(), 3 * TEST_CREATE_FILES_NUM);

        file_operator.remove_path(dir_path).unwrap();
    }

    fn build_local_test_config() -> PersistentConfig {
        let mut config_map = ConfigMap::default();
        config_map.insert("persistent.root", "/tmp/geaflow_cstore_local");
        config_map.insert("persistent.persistent_type", "Local");
        let cstore_config = CStoreConfigBuilder::default()
            .set_with_map(&config_map)
            .build();
        cstore_config.persistent
    }

    fn create_files_and_directories(
        operator: &OpenDalFileOperator,
        dir_path_str: &str,
        file_handle_type: &FileHandleType,
    ) {
        for i in 0..TEST_CREATE_FILES_NUM {
            let file_path = PathBuf::new()
                .join(dir_path_str)
                .join(i.to_string())
                .with_extension(TEST_FILE_EXTENSION);
            operator.create_file(&file_path).unwrap();
            let mut file = operator
                .open(file_handle_type, &file_path, OpenMode::WriteOnly)
                .unwrap();
            file.write(&vec![0u8; 3]).unwrap();
        }

        let child1_dir_path = PathBuf::new().join(dir_path_str).join("test1");

        for i in 0..TEST_CREATE_FILES_NUM {
            let file_path = PathBuf::new()
                .join(&child1_dir_path)
                .join(i.to_string())
                .with_extension(TEST_FILE_EXTENSION);
            operator.create_file(&file_path).unwrap();
            let mut file = operator
                .open(file_handle_type, &file_path, OpenMode::WriteOnly)
                .unwrap();
            file.write(&vec![0u8; 3]).unwrap();
        }

        let child2_dir_path = PathBuf::new()
            .join(dir_path_str)
            .join("test1")
            .join("test2");

        for i in 0..TEST_CREATE_FILES_NUM {
            let file_path = PathBuf::new()
                .join(&child2_dir_path)
                .join(i.to_string())
                .with_extension(TEST_FILE_EXTENSION);
            operator.create_file(&file_path).unwrap();
            let mut file = operator
                .open(file_handle_type, &file_path, OpenMode::WriteOnly)
                .unwrap();
            file.write(&vec![0u8; 3]).unwrap();
        }
    }

    #[test]
    fn test_oss_list_function() {
        let persistent_config = &build_oss_persistent_config("test_oss_list");
        let file_operator = OpenDalFileOperator::new(persistent_config).unwrap();

        let list_dir_1 = PathBuf::from(&persistent_config.root).join("test_list1/");
        test_list(
            &file_operator,
            &FileHandleType::Oss,
            list_dir_1.to_str().unwrap(),
        );

        let list_dir_2 = PathBuf::from(&persistent_config.root).join("test_list2/");
        test_list_file_relative_path(
            &file_operator,
            &FileHandleType::Oss,
            list_dir_2.to_str().unwrap(),
        );
    }

    #[test]
    fn test_oss_rename_dir() {
        let persistent_config = &build_oss_persistent_config("test_oss_rename");
        test_rename_dir(persistent_config);
    }

    pub fn check_dir_exist(operator: &OpenDalFileOperator, path: &Path) -> Result<bool> {
        let rel_dir_path = operator.build_rel_path(path);
        operator.is_dir_exist(&rel_dir_path)
    }

    #[test]
    fn test_oss_file() {
        let persistent_config = &build_oss_persistent_config("test_oss_file");
        let operator = OpenDalFileOperator::new(persistent_config).unwrap();

        let dir_path = Path::new(&persistent_config.root).join("test_oss_file/");
        let file_path = dir_path.join(TEST_FILE_NAME);

        operator.remove_path(&dir_path).unwrap();
        operator.create_file(&file_path).unwrap();

        let mut write_buffer: Vec<u8> = vec![0u8; TEST_DATA_LENGTH];
        let mut rng: IsaacRng = SeedableRng::from_entropy();
        rng.fill_bytes(&mut write_buffer);

        let mut file = operator
            .open(&FileHandleType::Oss, &file_path, OpenMode::WriteOnly)
            .unwrap();
        file.write(&write_buffer).unwrap();
        file.write(&write_buffer).unwrap();
        file.flush().unwrap();

        let mut read_buffer: Vec<u8> = vec![0u8; TEST_DATA_LENGTH];
        file.read(&mut read_buffer, 0, TEST_DATA_LENGTH as u32)
            .unwrap();

        let mut read_buffer2: Vec<u8> = vec![0u8; TEST_DATA_LENGTH];
        file.read(
            &mut read_buffer2,
            TEST_DATA_LENGTH as u64,
            TEST_DATA_LENGTH as u32 * 2,
        )
        .unwrap();

        assert!(bytes_are_equal(&write_buffer, &read_buffer));
        assert!(bytes_are_equal(&write_buffer, &read_buffer2));
        operator.remove_path(&dir_path).unwrap();
    }

    #[cfg(feature = "hdfs")]
    #[test]
    fn test_hdfs_rename_dir() {
        let persistent_config = &build_hdfs_persistent_config("test_hdfs_rename");
        test_rename_dir(persistent_config);
    }

    fn test_rename_dir(persistent_config: &PersistentConfig) {
        let operator = OpenDalFileOperator::new(persistent_config).unwrap();

        let src = PathBuf::from(&persistent_config.root).join("test_rename_dir_1/");
        let dst = PathBuf::from(&persistent_config.root).join("test_rename_dir_2/");

        operator.remove_path(&dst).unwrap();
        operator.create_dir(&src).unwrap();
        create_files_and_directories(&operator, src.to_str().unwrap(), &FileHandleType::Oss);

        operator.rename_dir(&src, &dst).unwrap();
        let src_files = operator
            .list_recursive_files("test_rename_dir_1/", true)
            .unwrap();
        assert_eq!(src_files.len(), 0);
        let dst_files = operator
            .list_recursive_files("test_rename_dir_2/", true)
            .unwrap();
        assert_eq!(dst_files.len(), 15);

        assert!(!check_dir_exist(&operator, &src).unwrap());
        assert!(check_dir_exist(&operator, &dst).unwrap());
        operator.remove_path(&dst).unwrap();
        assert!(!check_dir_exist(&operator, &dst).unwrap());
    }
}
