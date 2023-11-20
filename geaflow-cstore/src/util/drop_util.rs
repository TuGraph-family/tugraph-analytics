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

use std::path::PathBuf;

use rustc_hash::FxHashSet;
use strum_macros::{Display, EnumString};

use super::thread_util::RayonThreadPool;
use crate::{
    error_and_panic, file::file_handle::FileHandleType, lsm::level_controller::LevelController,
    table::build_file_name, CstoreFileOperator, TableType,
};

// File types and reasons for drop.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, EnumString, Display)]
pub enum DropFileType {
    #[default]
    UnDefined = 0,
    // The compacted index files and data files in namespace are dropped regularly. The namespace
    // here refers to the place used to store written indexes and data files.
    DropLocationDataAfterCompact,
    // When archiving, the snapshot files(include manifest, memory index manifest, dict snapshot,
    // etc) will be stored in the local namespace. After the archive is completed, these files
    // need to be dropped.
    DropLocalManifestAfterArchive,
    // After multiple archives, the number of snapshots in the persistent namespace will exceed
    // the configured maximum limit. In this case, expired snapshot files need to be dropped.
    DropPersistentManifestExpired,
    // When dropping an expired snapshot files, it's need to drop the index files and data files
    // in persistent namespace managed in the manifest files.
    DropPersistentDataExpired,
    // After archiving, check the files in the persistent namespace that are not managed by
    // manifest files and drop them.
    DropPersistentDataGhost,
}

// Get registered file information from the level controller, including index
// files and data files. Returns a set of registered file names and the largest
// file id.
pub fn get_registered_index_and_data_files_info(
    level_controller: &LevelController,
) -> (FxHashSet<String>, u32) {
    let mut registered_files: FxHashSet<String> = FxHashSet::default();
    let mut max_registered_fid: u32 = 0;

    for i in 0..level_controller.max_level() {
        level_controller
            .get_level_handler(i)
            .read()
            .unwrap()
            .get_table_info_map()
            .iter()
            .for_each(|(_index_fid, table_info)| {
                let index_table_fid = table_info.index_table_info.fid;
                let data_table_fid = table_info.data_table_info.fid;

                registered_files.insert(build_file_name(index_table_fid, TableType::Is));
                registered_files.insert(build_file_name(data_table_fid, TableType::Vs));

                max_registered_fid = u32::max(max_registered_fid, index_table_fid);
                max_registered_fid = u32::max(max_registered_fid, data_table_fid);
            })
    }

    (registered_files, max_registered_fid)
}

// Drop files with multiple threads.
//
// Support deleting files and directories. The pathname in pathname set is
// relative to the name space. E.g.  path_name_set = {"dict", "0.vs"},
// name_space = "/tmp/test"       Then the set of absolute paths that need
// to be deleted is {"/tmp/test/dict", "/tmp/test/0.vs"}
//
pub fn drop_files_multi_thread(
    thread_pool: &RayonThreadPool,
    file_handle_type: &FileHandleType,
    path_name_set: &FxHashSet<String>,
    name_space: &str,
    file_operator: &CstoreFileOperator,
    drop_file_type: DropFileType,
) {
    thread_pool.iter_execute(path_name_set, |path_name| {
        let pathbuf = PathBuf::new().join(name_space).join(path_name);
        file_operator
            .remove_path(file_handle_type, pathbuf.as_path())
            .unwrap_or_else(|e| {
                error_and_panic!("drop file type: {}, {}", drop_file_type, e);
            });
    });
}
