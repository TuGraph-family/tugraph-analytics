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

use std::{fs, fs::OpenOptions, path::Path};

use memmap2::MmapMut;
use rustc_hash::FxHashSet;

use super::FileOperator;
use crate::{
    error::Error,
    file::file_handle::{
        FileHandle, FileHandleType, MmapFile, OpenMode, StdFile, DEFAULT_EXTEND_LENGTH,
    },
    util::string_util::get_rfind_front_section,
    Result, FILE_PATH_DELIMITER,
};

#[derive(Debug, Default)]
pub struct LocalFileOperator;

impl LocalFileOperator {
    // List relative path of the files in specified directory recursively.
    fn list_file_recursively(
        dir: &Path,
        base_dir: &Path,
        file_set: &mut FxHashSet<String>,
    ) -> Result<()> {
        if dir.is_dir() {
            for entry in fs::read_dir(dir).map_err(|e| {
                Error::LocalFileOperatorError(dir.to_str().unwrap().to_string(), e.to_string())
            })? {
                let path = entry
                    .map_err(|e| {
                        Error::LocalFileOperatorError(
                            dir.to_str().unwrap().to_string(),
                            e.to_string(),
                        )
                    })?
                    .path();

                if path.is_dir() {
                    Self::list_file_recursively(&path, base_dir, file_set)?;
                } else {
                    let relative_path = path.strip_prefix(base_dir)?;
                    file_set.insert(relative_path.to_str().unwrap().to_string());
                }
            }
        }

        Ok(())
    }
}

impl FileOperator for LocalFileOperator {
    fn open(
        &self,
        file_handle_type: &FileHandleType,
        file_path: &Path,
        _open_mode: OpenMode,
    ) -> Result<Box<dyn FileHandle>> {
        match file_handle_type {
            FileHandleType::Std(_) => {
                let mut options: OpenOptions = OpenOptions::new();
                options.read(true).write(true);

                let file = options.open(file_path).map_err(|e| {
                    Error::LocalFileOperatorError(
                        file_path.to_str().unwrap().to_string(),
                        e.to_string(),
                    )
                })?;

                Ok(Box::new(StdFile::new(file, file_path)))
            }

            FileHandleType::Mmap(_) => {
                let mut options: OpenOptions = OpenOptions::new();
                options.read(true).write(true);
                let file: fs::File = options.open(file_path).map_err(|e| {
                    Error::LocalFileOperatorError(
                        file_path.to_str().unwrap().to_string(),
                        e.to_string(),
                    )
                })?;

                let file_len = file
                    .metadata()
                    .map_err(|e| {
                        Error::LocalFileOperatorError(
                            file_path.to_str().unwrap().to_string(),
                            e.to_string(),
                        )
                    })?
                    .len();

                let mmap = if file_len == 0 {
                    file.set_len(DEFAULT_EXTEND_LENGTH).map_err(|e| {
                        Error::LocalFileOperatorError(
                            file_path.to_str().unwrap().to_string(),
                            e.to_string(),
                        )
                    })?;

                    unsafe { MmapMut::map_mut(&file).unwrap() }
                } else {
                    unsafe { MmapMut::map_mut(&file).unwrap() }
                };

                Ok(Box::new(MmapFile::new(file, mmap, file_path)))
            }

            #[allow(unused)]
            _ => unreachable!(),
        }
    }

    fn create_file(&self, file_path: &Path) -> Result<()> {
        let file_path_str = file_path.to_str().unwrap();
        let dir_path_str = get_rfind_front_section(file_path_str, FILE_PATH_DELIMITER);

        fs::create_dir_all(dir_path_str).map_err(|e| {
            Error::LocalFileOperatorError(file_path.to_str().unwrap().to_string(), e.to_string())
        })?;

        OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(file_path)
            .map_err(|e| {
                Error::LocalFileOperatorError(
                    file_path.to_str().unwrap().to_string(),
                    e.to_string(),
                )
            })?;

        Ok(())
    }

    fn create_dir(&self, dir_path: &Path) -> Result<()> {
        fs::create_dir_all(dir_path).map_err(|e| {
            Error::LocalFileOperatorError(dir_path.to_str().unwrap().to_string(), e.to_string())
        })?;

        Ok(())
    }

    fn remove_path(&self, path: &Path) -> Result<()> {
        if path.exists() {
            if path.is_dir() {
                fs::remove_dir_all(path).map_err(|e| {
                    Error::LocalFileOperatorError(path.to_str().unwrap().to_string(), e.to_string())
                })?;
            } else {
                fs::remove_file(path).map_err(|e| {
                    Error::LocalFileOperatorError(path.to_str().unwrap().to_string(), e.to_string())
                })?;
            }
        }

        Ok(())
    }

    fn remove_file(&self, path: &Path) -> Result<()> {
        if path.exists() {
            fs::remove_file(path).map_err(|e| {
                Error::LocalFileOperatorError(path.to_str().unwrap().to_string(), e.to_string())
            })?;
        }

        Ok(())
    }

    fn rename_dir(&self, src: &Path, dst: &Path) -> Result<()> {
        fs::rename(src, dst).map_err(|e| {
            Error::LocalFileOperatorError(src.to_str().unwrap().to_string(), e.to_string())
        })?;

        Ok(())
    }

    fn list(&self, dir_path: &Path, recursively: bool) -> Result<FxHashSet<String>> {
        let mut file_names: FxHashSet<String> = FxHashSet::default();

        if !recursively {
            if dir_path.exists() {
                for entry in fs::read_dir(dir_path).map_err(|e| {
                    Error::LocalFileOperatorError(
                        dir_path.to_str().unwrap().to_string(),
                        e.to_string(),
                    )
                })? {
                    file_names.insert(
                        entry
                            .map_err(|e| {
                                Error::LocalFileOperatorError(
                                    dir_path.to_str().unwrap().to_string(),
                                    e.to_string(),
                                )
                            })?
                            .file_name()
                            .to_string_lossy()
                            .to_string(),
                    );
                }
            }
        } else {
            Self::list_file_recursively(dir_path, dir_path, &mut file_names)?;
        }

        Ok(file_names)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_local_rename_dir() {
        let src = "/tmp/geaflow_cstore_local/test_rename_dir_1";
        let dst = "/tmp/geaflow_cstore_local/test_rename_dir_2";

        LocalFileOperator.remove_path(Path::new(dst)).unwrap();
        LocalFileOperator.create_dir(Path::new(src)).unwrap();
        LocalFileOperator
            .rename_dir(Path::new(src), Path::new(dst))
            .unwrap();

        assert!(Path::new(dst).exists());

        LocalFileOperator.remove_path(Path::new(dst)).unwrap();
        assert!(!Path::new(dst).exists());
    }
}
