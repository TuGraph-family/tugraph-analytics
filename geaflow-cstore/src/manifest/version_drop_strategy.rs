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
    path::Path,
    str::FromStr,
    sync::{Arc, RwLock},
};

use dashmap::DashSet;
use itertools::Itertools;
use rustc_hash::{FxHashMap, FxHashSet};

use super::version_controller::VersionControllerInner;
use crate::{
    cache::CStoreCacheHandler,
    error_and_panic,
    file::{file_handle::FileHandleType, file_operator::CstoreFileOperator},
    gen::manifest::Manifest,
    log_util::{debug, info},
    lsm::level_controller::LevelController,
    table::{build_file_name, get_table_type},
    util::{
        drop_util::{
            drop_files_multi_thread, get_registered_index_and_data_files_info, DropFileType,
        },
        number_util::{get_integer_from_str_prefix, get_integer_from_str_suffix},
        string_util::get_rfind_back_section,
        thread_util::RayonThreadPool,
    },
    TableType, CHECK_POINT_SUFFIX, DATA_AND_INDEX_FILE_NAME_DELIMITER,
    MANIFEST_FILE_NAME_DELIMITER,
};

pub struct ManifestDropStrategy {}

// Drop strategy of manifest.
impl ManifestDropStrategy {
    // Drop files in name space after finishing archiving.
    pub fn drop_local_name_space_after_archive(vc_inner: &VersionControllerInner) {
        let file_handle_type = FileHandleType::default();
        let name_space = &vc_inner.local_name_space;

        let file_operator = &vc_inner.file_operator;
        let to_drop_files: FxHashSet<String> = file_operator
            .list(&file_handle_type, Path::new(name_space), false)
            .unwrap_or_else(|e| error_and_panic!("{}", e));

        // Remove files included in reserve files.
        let mut drop_manifest_file_name_set: FxHashSet<String> = FxHashSet::default();

        // Remove fid greater than the maximum fid at the beginning of current archive.
        for file_name in to_drop_files.into_iter() {
            // Drop snapshot of id dict in local namespace.
            if file_name.contains(&vc_inner.id_dict_key) && !file_name.contains(CHECK_POINT_SUFFIX)
            {
                continue;
            }

            match get_table_type(&file_name) {
                Ok(table_type) => {
                    if !table_type.is_index_or_data_table() {
                        drop_manifest_file_name_set.insert(file_name);
                    }
                }

                Err(_) => {
                    drop_manifest_file_name_set.insert(file_name);
                }
            }
        }

        drop_files_multi_thread(
            &vc_inner.thread_pool,
            &file_handle_type,
            &drop_manifest_file_name_set,
            name_space,
            file_operator,
            DropFileType::DropLocalManifestAfterArchive,
        );

        info!(
            "finish dropping manifest files from local name space after archive. Drop manifest files {:?}",
            drop_manifest_file_name_set
        );
    }

    // Drop expired manifest files, index files and data files in persistent name
    // space after finishing archiving.
    pub fn drop_persistent_name_space_after_archive(
        vc_inner: &VersionControllerInner,
        cache_handler: &CStoreCacheHandler,
        last_manifest: &Option<Manifest>,
        current_manifest: &Option<Manifest>,
        level_controller: &LevelController,
        compact_drop_lock: &RwLock<()>,
    ) {
        let registered_files: FxHashSet<String>;
        let max_registered_fid: u32;
        let mut index_data_file_set: FxHashSet<String>;
        let mut manifest_version_vec: Vec<u64>;

        {
            let _compact_drop_write_guard = compact_drop_lock.write().unwrap();
            (index_data_file_set, manifest_version_vec) =
                Self::get_persistent_name_space_drop_info(vc_inner);

            (registered_files, max_registered_fid) =
                get_registered_index_and_data_files_info(level_controller);
        }

        // Drop eliminated files when number of manifest exceeds maximum limit.
        let (drop_version, drop_index_data_file_name_set, drop_manifest_file_name_set) =
            Self::drop_expired_files(&mut manifest_version_vec, vc_inner, cache_handler);

        index_data_file_set.retain(|file| {
            !drop_index_data_file_name_set.contains(file) && !registered_files.contains(file)
        });

        // Delete ghost index and data files that does not appear in any manifest.
        Self::drop_ghost_files(
            &mut index_data_file_set,
            &mut manifest_version_vec,
            vc_inner,
            cache_handler,
            last_manifest,
            current_manifest,
            max_registered_fid,
        );

        info!(
            "finish dropping files from persistent name space after archive. Drop expired version {:?} index and data files {:?} manifest_files {:?}. Drop ghost index and data files {:?}",
            drop_version,
            drop_index_data_file_name_set,
            drop_manifest_file_name_set,
            index_data_file_set
        );
        // No need to drop.
    }

    fn get_persistent_name_space_drop_info(
        vc_inner: &VersionControllerInner,
    ) -> (FxHashSet<String>, Vec<u64>) {
        let persistent_file_handle_type = &vc_inner.persistent_file_handle_type;
        let persistent_name_space = &vc_inner.persistent_name_space;

        let file_operator = &vc_inner.file_operator;
        let all_file_set: FxHashSet<String> = file_operator
            .list(
                persistent_file_handle_type,
                Path::new(persistent_name_space),
                false,
            )
            .unwrap_or_else(|e| error_and_panic!("{}", e));

        let mut manifest_file_set: FxHashSet<String> = FxHashSet::default();
        let mut index_data_file_set: FxHashSet<String> = FxHashSet::default();

        all_file_set.into_iter().for_each(|file_name| {
            let extension = get_rfind_back_section(&file_name, DATA_AND_INDEX_FILE_NAME_DELIMITER);
            if extension == file_name {
                if extension.contains(MANIFEST_FILE_NAME_DELIMITER) {
                    manifest_file_set.insert(file_name);
                    return;
                } else if extension.contains(CHECK_POINT_SUFFIX) {
                    return;
                }
            }

            let table_type = TableType::from_str(extension).unwrap_or_default();

            if table_type.is_index_or_data_table() {
                index_data_file_set.insert(file_name);
            }
        });

        let manifest_version_vec = Self::get_manifest_version_vec(&manifest_file_set);

        (index_data_file_set, manifest_version_vec)
    }

    #[allow(clippy::too_many_arguments)]
    fn drop_ghost_files(
        to_drop_index_data_file_set: &mut FxHashSet<String>,
        manifest_version_vec: &mut Vec<u64>,
        vc_inner: &VersionControllerInner,
        cache_handler: &CStoreCacheHandler,
        last_manifest: &Option<Manifest>,
        current_manifest: &Option<Manifest>,
        max_registered_fid: u32,
    ) {
        if to_drop_index_data_file_set.is_empty() {
            return;
        }

        let registered_file_set: FxHashSet<String> = Self::get_registered_files(
            manifest_version_vec,
            vc_inner,
            cache_handler,
            last_manifest,
            current_manifest,
        );

        debug!(
            "index and data file set {:?}, registered file set {:?}, max_registered_fid {}",
            to_drop_index_data_file_set, registered_file_set, max_registered_fid
        );

        to_drop_index_data_file_set.retain(|item| {
            let fid: u32 = get_integer_from_str_prefix(item, DATA_AND_INDEX_FILE_NAME_DELIMITER)
                .unwrap_or(u32::MAX);

            !registered_file_set.contains(item) && fid < max_registered_fid
        });

        let to_drop_index_data_file_set: &FxHashSet<String> = to_drop_index_data_file_set;

        drop_files_multi_thread(
            &vc_inner.thread_pool,
            &vc_inner.persistent_file_handle_type,
            to_drop_index_data_file_set,
            &vc_inner.persistent_name_space,
            &vc_inner.file_operator,
            DropFileType::DropPersistentDataGhost,
        );
    }

    fn get_registered_files(
        manifest_version_vec: &mut Vec<u64>,
        vc_inner: &VersionControllerInner,
        cache_handler: &CStoreCacheHandler,
        last_manifest: &Option<Manifest>,
        current_manifest: &Option<Manifest>,
    ) -> FxHashSet<String> {
        let mut register_file_set: FxHashSet<String> = FxHashSet::default();

        if last_manifest.is_some() {
            register_file_set.extend(Self::get_index_data_files_by_manifest(
                last_manifest.as_ref().unwrap(),
            ));
            info!(
                "last_manifest version {}",
                last_manifest.as_ref().unwrap().version
            );
            manifest_version_vec
                .retain(|version| *version != last_manifest.as_ref().unwrap().version)
        }

        if current_manifest.is_some() {
            register_file_set.extend(Self::get_index_data_files_by_manifest(
                current_manifest.as_ref().unwrap(),
            ));
            info!(
                "current_manifest version {}",
                current_manifest.as_ref().unwrap().version
            );
            manifest_version_vec
                .retain(|version| *version != current_manifest.as_ref().unwrap().version)
        }

        let register_file_set_by_manifest_cache: FxHashSet<String> =
            Self::get_registered_files_by_manifest_cache(
                manifest_version_vec,
                vc_inner,
                cache_handler,
            );

        register_file_set.extend(register_file_set_by_manifest_cache);

        register_file_set
    }

    fn get_registered_files_by_manifest_cache(
        manifest_version_vec: &mut Vec<u64>,
        vc_inner: &VersionControllerInner,
        cache_handler: &CStoreCacheHandler,
    ) -> FxHashSet<String> {
        let registered_files: DashSet<String> = DashSet::default();

        vc_inner
            .thread_pool
            .iter_execute(manifest_version_vec, |fid| {
                let manifest = cache_handler
                    .manifest_cache
                    .get_with(fid, vc_inner)
                    .unwrap_or_else(|e| {
                        error_and_panic!("{}", e);
                    });

                let register_file_set: FxHashSet<String> =
                    Self::get_index_data_files_by_manifest(&manifest);

                register_file_set.into_iter().for_each(|file_name| {
                    registered_files.insert(file_name);
                })
            });

        registered_files.into_iter().collect()
    }

    fn drop_expired_files(
        manifest_version_vec: &mut Vec<u64>,
        vc_inner: &VersionControllerInner,
        cache_handler: &CStoreCacheHandler,
    ) -> (Option<u64>, FxHashSet<String>, FxHashSet<String>) {
        let persistent_file_handle_type = &vc_inner.persistent_file_handle_type;
        let persistent_name_space = &vc_inner.persistent_name_space;
        if manifest_version_vec.len() <= vc_inner.max_manifest_num {
            return (None, FxHashSet::default(), FxHashSet::default());
        }

        let drop_version = manifest_version_vec.remove(0);
        let compare_version = manifest_version_vec.first();

        let compare_version = *compare_version.unwrap();

        // Get the manifest to drop and the manifest to be compared.
        let (drop_manifest, compare_manifest) = Self::get_drop_and_compare_manifest(
            cache_handler,
            &drop_version,
            &compare_version,
            vc_inner,
        );

        // Get index and data files to drop by comparing two manifests.
        let drop_index_data_file_name_set: FxHashSet<String> =
            Self::get_drop_index_data_files_by_compare(&drop_manifest, &compare_manifest);

        // Drop index files and data files.
        drop_files_multi_thread(
            &vc_inner.thread_pool,
            persistent_file_handle_type,
            &drop_index_data_file_name_set,
            persistent_name_space,
            &vc_inner.file_operator,
            DropFileType::DropPersistentDataExpired,
        );

        let drop_manifest_file_name_set: FxHashSet<String> = Self::drop_manifest_files(
            &vc_inner.thread_pool,
            &drop_manifest,
            persistent_file_handle_type,
            persistent_name_space,
            &vc_inner.file_operator,
        );

        (
            Some(drop_version),
            drop_index_data_file_name_set,
            drop_manifest_file_name_set,
        )
    }

    fn get_drop_and_compare_manifest(
        cache_handler: &CStoreCacheHandler,
        drop_version: &u64,
        compare_version: &u64,
        vc_inner: &VersionControllerInner,
    ) -> (Arc<Manifest>, Arc<Manifest>) {
        // Get the manifest to drop, which would be compared with another manifest.
        let drop_manifest = cache_handler
            .manifest_cache
            .get_with(drop_version, vc_inner)
            .unwrap_or_else(|e| {
                error_and_panic!("{}", e);
            });

        // Get the manifest to compare with the manifest to drop.
        let compare_manifest = cache_handler
            .manifest_cache
            .get_with(compare_version, vc_inner)
            .unwrap_or_else(|e| {
                error_and_panic!("{}", e);
            });

        (drop_manifest, compare_manifest)
    }

    fn drop_manifest_files(
        thread_pool: &RayonThreadPool,
        drop_manifest: &Manifest,
        drop_file: &FileHandleType,
        drop_name_space: &str,
        file_operator: &CstoreFileOperator,
    ) -> FxHashSet<String> {
        let mut drop_manifest_file_name_set: FxHashSet<String> = FxHashSet::default();

        // Add snapshot of manifest to drop set.
        drop_manifest_file_name_set.insert(build_file_name(drop_manifest.version, TableType::Ms));

        // Add snapshot of memory index manifest to drop set.
        drop_manifest_file_name_set.insert(build_file_name(
            drop_manifest.memory_index_fid,
            TableType::Mi,
        ));

        // Add snapshot of id dict to drop set.
        let id_dict_snapshot_path = Path::new(&drop_manifest.id_dict_snapshot_path);
        let id_dict_snapshot_relative_path = id_dict_snapshot_path
            .strip_prefix(drop_name_space)
            .unwrap()
            .to_str()
            .unwrap()
            .to_string();
        drop_manifest_file_name_set.insert(id_dict_snapshot_relative_path);

        // Drop ManifestFiles.
        drop_files_multi_thread(
            thread_pool,
            drop_file,
            &drop_manifest_file_name_set,
            drop_name_space,
            file_operator,
            DropFileType::DropPersistentManifestExpired,
        );

        drop_manifest_file_name_set
    }

    fn get_drop_index_data_files_by_compare(
        drop_manifest: &Manifest,
        compare_manifest: &Manifest,
    ) -> FxHashSet<String> {
        // Record fid -> TableType
        let mut drop_map: FxHashMap<u32, u32> = FxHashMap::default();

        for level in 0..drop_manifest.level_manifest_vec.len() {
            let drop_manifest_level_handler = drop_manifest.level_manifest_vec.get(level).unwrap();
            let compare_manifest_level_handler =
                compare_manifest.level_manifest_vec.get(level).unwrap();

            let mut tmp_drop_index_map: FxHashMap<u32, u32> = FxHashMap::default();
            let mut tmp_drop_data_map: FxHashMap<u32, u32> = FxHashMap::default();

            let mut tmp_compare_index_fid_set: FxHashSet<u32> = FxHashSet::default();
            let mut tmp_compare_data_fid_set: FxHashSet<u32> = FxHashSet::default();

            drop_manifest_level_handler
                .table_info_manifest_map
                .iter()
                .map(|item| item.1)
                .for_each(|table_info| {
                    // TODO change value -> table_manifest.table_type
                    tmp_drop_index_map
                        .insert(table_info.index_table_manifest.as_ref().unwrap().fid, 1);
                    tmp_drop_data_map
                        .insert(table_info.data_table_manifest.as_ref().unwrap().fid, 2);
                });

            compare_manifest_level_handler
                .table_info_manifest_map
                .iter()
                .map(|item| item.1)
                .for_each(|table_info| {
                    tmp_compare_index_fid_set
                        .insert(table_info.index_table_manifest.as_ref().unwrap().fid);
                    tmp_compare_data_fid_set
                        .insert(table_info.data_table_manifest.as_ref().unwrap().fid);
                });

            tmp_compare_index_fid_set.iter().for_each(|fid| {
                if tmp_drop_index_map.contains_key(fid) {
                    tmp_drop_index_map.remove(fid);
                }
            });

            tmp_compare_data_fid_set.iter().for_each(|fid| {
                if tmp_drop_data_map.contains_key(fid) {
                    tmp_drop_data_map.remove(fid);
                }
            });

            drop_map.extend(tmp_drop_index_map);
            drop_map.extend(tmp_drop_data_map);
        }

        // Initialize with files that need to be dropped.
        let drop_file_name_set: FxHashSet<String> = drop_map
            .iter()
            .map(|drop_info| build_file_name(*drop_info.0, TableType::from(*drop_info.1)))
            .collect();

        drop_file_name_set
    }

    fn get_index_data_files_by_manifest(manifest: &Manifest) -> FxHashSet<String> {
        // Record fid -> TableType
        let mut file_map: FxHashMap<u32, u32> = FxHashMap::default();

        for level in 0..manifest.level_manifest_vec.len() {
            let drop_manifest_level_handler = manifest.level_manifest_vec.get(level).unwrap();

            let mut tmp_index_file_map: FxHashMap<u32, u32> = FxHashMap::default();
            let mut tmp_data_file_map: FxHashMap<u32, u32> = FxHashMap::default();

            drop_manifest_level_handler
                .table_info_manifest_map
                .iter()
                .map(|item| item.1)
                .for_each(|table_info| {
                    // TODO change value -> table_manifest.table_type
                    tmp_index_file_map
                        .insert(table_info.index_table_manifest.as_ref().unwrap().fid, 1);
                    tmp_data_file_map
                        .insert(table_info.data_table_manifest.as_ref().unwrap().fid, 2);
                });

            file_map.extend(tmp_index_file_map);
            file_map.extend(tmp_data_file_map);
        }

        let file_name_set: FxHashSet<String> = file_map
            .iter()
            .map(|drop_info| build_file_name(*drop_info.0, TableType::from(*drop_info.1)))
            .collect();

        file_name_set
    }

    fn get_manifest_version_vec(manifest_file_set: &FxHashSet<String>) -> Vec<u64> {
        let mut manifest_version_vec = manifest_file_set
            .iter()
            .map(|file_name| {
                let version: u64 =
                    get_integer_from_str_suffix(file_name, MANIFEST_FILE_NAME_DELIMITER).unwrap();
                version
            })
            .collect_vec();

        manifest_version_vec.sort();
        manifest_version_vec
    }
}
