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

#![allow(non_upper_case_globals, unused, clippy::too_many_arguments)]

use core::fmt;
use std::{
    collections::HashMap,
    default,
    fmt::Formatter,
    fs::FileType,
    num::NonZeroUsize,
    path::{Path, PathBuf},
    rc::Rc,
    str::FromStr,
    sync::{
        mpsc::{self, Sender},
        Arc, Mutex, RwLock,
    },
    thread,
    time::Instant,
};

use itertools::Itertools;
use num_enum::{IntoPrimitive, TryFromPrimitive};
use prost::Message;
use rustc_hash::{FxHashMap, FxHashSet};
use sha2::digest::typenum::NonZero;
use strum_macros::{Display, EnumString};

use super::{
    manifest_manager::ManifestManager,
    version_drop_strategy::{self, ManifestDropStrategy},
};
use crate::{
    cache::CStoreCacheHandler,
    config::{DictConfig, ManifestConfig, TableConfig},
    convert_u64_to_usize,
    engine::{
        dict::{
            id_dict::{IdDict, RockDbIdDict},
            label_dict::LabelDict,
            ttl_dict::TtlDict,
            DictUtil,
        },
        sequence_id::SequenceId,
    },
    error::Error,
    error_and_panic,
    file::{
        file_copy::{CopyFileType, FileCopyHandler},
        file_handle::FileHandleType,
        file_operator::CstoreFileOperator,
    },
    gen::manifest::{
        DataTableInfoManifest, IndexTableInfoManifest, LevelManifest, Manifest, TableInfoManifest,
    },
    index::{csr_index::CsrIndex, index_backup_executor::CsrIndexBackupExecutor, parent_node},
    lsm::level_controller::LevelController,
    table::build_file_name,
    util::{
        number_util::get_integer_from_str_suffix, thread_util::RayonThreadPool,
        time_util::time_now_as_secs,
    },
    with_bound, Result, TableType, MANIFEST_FILE_NAME_DELIMITER, MANIFEST_FILE_NAME_PREFIX,
};

// ArchiveTimeCost, which is used to record time-consuming information of
// archiving in version controller.
#[derive(Debug, Default)]
pub struct ArchiveTimeCost {
    pub all_time: u128,
    pub snapshot_manifest_time: u128,
    pub snapshot_others_time: u128,
    pub build_upload_info_time: u128,
    pub upload_time: u128,
    pub drop_persistent_time: u128,
    pub drop_local_time: u128,
}

impl fmt::Display for ArchiveTimeCost {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "all_time: {}ms, snapshot_manifest_time: {}ms, snapshot_others_time: {}ms, build_upload_info_time: {}ms, upload_time: {}ms, drop_local_time: {}ms, drop_persistent_time: {}ms",
            self.all_time,
            self.snapshot_manifest_time,
            self.snapshot_others_time,
            self.build_upload_info_time,
            self.upload_time,
            self.drop_local_time,
            self.drop_persistent_time
        )
    }
}

// RecoverTimeCost, which is used to record time-consuming information of
// recovery in version controller.
#[derive(Debug, Default)]
pub struct LoadTimeCost {
    pub all_time: u128,
    pub load_manifest_snapshot_time: u128,
    pub load_other_snapshots_time: u128,
    pub download_time: u128,
}

impl fmt::Display for LoadTimeCost {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "all_time: {}ms, load_manifest_snapshot_time: {}ms, load_other_snapshots_time: {}ms, download_time: {}ms",
            self.all_time,
            self.load_manifest_snapshot_time,
            self.load_other_snapshots_time,
            self.download_time
        )
    }
}

#[derive(Debug, Default)]
pub struct VersionControlLogInfo {
    pub archive_time_cost: ArchiveTimeCost,
    pub load_time_cost: LoadTimeCost,
}

// Meta data of the file needed to copy or not.
//
// The source namespace and destination namespace of each file can be obtained
// through the file handle type and archive file handle type, and then the
// source path and destination path of the file are generated.
//
// E.g.
// local_name_space = "/tmp/local", persistent_name_space = "/tmp/remote"
// file_name = "test.vs", src_file_handle_type = Std(Local),
// dest_file_handle_type = Hdfs
//
// Which means files would be read from local name space with file handle of
// type Std, copied and written to persistent name space with file handle of
// type Hdfs, and source path is "/tmp/local/test.vs", destination path is
// "/tmp/remote/test.vs".
#[derive(Eq, Hash, PartialEq)]
pub struct CopyFileMeta {
    pub file_name: String,
    pub src_file_handle_type: FileHandleType,
    pub dest_file_handle_type: FileHandleType,
}

// Map from fid of table to type of table.
//
// Help to compare two versions of manifest files to derive new index files and
// value files, which are used to generate meta of file that needs to be
// uploaded and saved.
type FidToTableType = FxHashMap<u32, TableType>;

// VersionControllerInner stores the static parameters and function class
// handles in manifest manager.
//
// Semantics of local name space and persistent name space are the same as in
// configuration.
#[derive(Debug, Default)]
pub struct VersionControllerInner {
    pub local_name_space: String,
    pub persistent_name_space: String,

    pub id_dict_key: String,

    // TODO vertex and edge separation, index_file_handle_type, data_file_handle_type
    pub file_handle_type: FileHandleType,
    pub persistent_file_handle_type: FileHandleType,

    pub file_copy_handle: FileCopyHandler,
    pub max_manifest_num: usize,

    pub thread_pool: RayonThreadPool,
    pub file_operator: Arc<CstoreFileOperator>,
}

// Macro to generate with method for specified members in
// VersionControllerInner.
with_bound!(VersionControllerInner, self, id_dict_key: String, local_name_space: String, persistent_name_space: String,
            file_handle_type: FileHandleType, persistent_file_handle_type: FileHandleType, file_copy_handle: FileCopyHandler,
            max_manifest_num: usize, thread_pool: RayonThreadPool, file_operator: Arc<CstoreFileOperator>);

// ManifestManager.
//
// Its functions include data backup and recovery, version management.
//
// 1. Supports archiving to multiple versions.
// 2. Supports recovering from optional versions.
// 3. Supports auto-drop strategy, Including limiting the number of manifest files and dropping
//    ghost files
// 4. Supports two mode: LocalToRemote & RemoteToRemote. backup(snapshot) of manifest files and
//    dicts would be generated and written to local name space, and archived to persistent name
//    space, and index files and data files are written to the local name space or persistent name
//    space decided by the mode, which is archived to persistent name space in mode of LocalToRemote
//    and do nothing in mode of RemoteToRemote.
pub struct VersionController {
    // It is used multi-thread in manifest manager.
    vc_inner: VersionControllerInner,

    manifest_manager: ManifestManager,

    pub vc_log_info: VersionControlLogInfo,
}

impl VersionController {
    // Build a new manifest manager.
    pub fn new(
        manifest_config: &ManifestConfig,
        table_config: &TableConfig,
        dict_config: &DictConfig,
        file_operator: Arc<CstoreFileOperator>,
    ) -> Self {
        // TODO vertex and edge separation, index_file_handle_type,
        // data_file_handle_type
        Self {
            vc_inner: VersionControllerInner::default()
                .with_file_handle_type(table_config.base.file_handle_type)
                .with_persistent_file_handle_type(manifest_config.base.persistent_file_handle_type)
                .with_local_name_space(table_config.base.local_name_space.clone())
                .with_persistent_name_space(table_config.base.persistent_name_space.clone())
                .with_file_copy_handle(FileCopyHandler::new(
                    manifest_config.copy_buffer_size,
                    Arc::clone(&file_operator),
                ))
                .with_max_manifest_num(manifest_config.max_manifest_num)
                .with_thread_pool(RayonThreadPool::new(manifest_config.multi_work_threads_num))
                .with_id_dict_key(dict_config.id_dict_key.clone())
                .with_file_operator(Arc::clone(&file_operator)),

            manifest_manager: ManifestManager::default(),

            vc_log_info: VersionControlLogInfo::default(),
        }
    }

    // Archive information and data of version to persistent name space.
    //
    // The process is as follows:
    //
    // 1. Check version validity.
    // 2. Generate version snapshot (list, memory index list, dict snapshot, etc.).
    // 3. Upload files, including version information files and data files.
    // 4. Delete abandoned and expired files.
    pub fn archive(
        &mut self,
        version: u64,
        seq_id: u64,
        csr_index: &CsrIndex,
        level_controller: &LevelController,
        cache_handler: &CStoreCacheHandler,
        id_dict: &RockDbIdDict,
        label_dict: &LabelDict,
        compact_drop_lock: &RwLock<()>,
    ) {
        let start_archive = Instant::now();
        // Check the version to archive.
        self.check_archived(version).unwrap_or_else(|e| {
            error_and_panic!("{}", e);
        });

        // Get all the files to upload.
        let mut upload_info: FxHashSet<CopyFileMeta> = FxHashSet::default();
        let mut max_registered_fid = 0;

        // Build snapshot and write them to local name space.
        // Include manifest, manifest for memory index and snapshot for dict.
        self.snapshot(
            version,
            seq_id,
            csr_index,
            level_controller,
            &mut upload_info,
            id_dict,
            label_dict,
            &mut max_registered_fid,
        );

        // Build information for uploading and dropping.
        // Include upload information, data files and index files that need to be
        // retained after archiving.
        let start_build_upload_info = Instant::now();

        self.build_upload_info(&mut upload_info);

        self.vc_log_info.archive_time_cost.build_upload_info_time =
            start_build_upload_info.elapsed().as_millis();

        // Upload files by upload information.
        // Upload info contains the copy metadata of each file that needs to be
        // archived, through which the source path, destination path, read file handle,
        // and write (copy) file handle can be generated for uploading.
        let start_upload = Instant::now();
        self.vc_inner.file_copy_handle.copy_files(
            &self.vc_inner.thread_pool,
            &upload_info,
            &self.vc_inner.local_name_space,
            &self.vc_inner.persistent_name_space,
            CopyFileType::VersionControlArchive,
        );
        self.vc_log_info.archive_time_cost.upload_time = start_upload.elapsed().as_millis();

        // Drop abandoned files after completing an archive.
        // 1. Drop files in persistent name space after completing an archive, include expired
        //    files, ghost files(not registered in any manifest in persistent name space).
        // 2. Drop files in local name space after completing an archive, include manifest, snapshot
        //    for memory index and dict.
        self.drop_after_archive(
            max_registered_fid,
            cache_handler,
            level_controller,
            compact_drop_lock,
        );

        // Clear current manifest and update the last manifest.
        let current_manifest = self.manifest_manager.take_current_manifest().unwrap();

        self.manifest_manager.update_last_manifest(current_manifest);
        self.vc_log_info.archive_time_cost.all_time = start_archive.elapsed().as_millis();
    }

    // Recover information and data of version from persistent name space.
    //
    // The process is as follows:
    // 1. Check version validity.
    // 2. Load the manifest and obtain download information of other version information files and
    //    data files.
    // 3. Download files, including version information files and data files.
    // 4. Load other version information files and snapshot.
    pub fn recover(
        &mut self,
        version: u64,
        csr_index: &CsrIndex,
        level_controller: &LevelController,
        id_dict: &RockDbIdDict,
        label_dict: &LabelDict,
        sequence_id_container: &SequenceId,
    ) {
        let start_load = Instant::now();
        // Check the version to load.
        self.check_loaded(version).unwrap_or_else(|e| {
            error_and_panic!("{}", e);
        });

        // Load the manifest and build information for downloading.
        let start_load_manifest_snapshot = Instant::now();
        let (download_info, load_snapshots_info) = self.manifest_manager.load_manifest_snapshot(
            &self.vc_inner,
            version,
            level_controller,
            sequence_id_container,
        );
        self.vc_log_info.load_time_cost.load_manifest_snapshot_time =
            start_load_manifest_snapshot.elapsed().as_millis();

        // Download files by download info.
        // Download info contains the copy metadata of each file that needs to be
        // loaded, through which the source path, destination path, read file handle,
        // and write (copy) file handle can be generated for downloading.
        let start_download = Instant::now();
        self.vc_inner.file_copy_handle.copy_files(
            &self.vc_inner.thread_pool,
            &download_info,
            &self.vc_inner.local_name_space,
            &self.vc_inner.persistent_name_space,
            CopyFileType::VersionControlRecover,
        );
        self.vc_log_info.load_time_cost.download_time = start_download.elapsed().as_millis();

        // Load other snapshot.
        // Include manifest for memory index and snapshot for dict.
        let start_load_other_snapshots = Instant::now();
        ManifestManager::load_other_snapshot(
            &self.vc_inner,
            csr_index,
            &load_snapshots_info,
            id_dict,
            label_dict,
        );
        self.vc_log_info.load_time_cost.load_other_snapshots_time =
            start_load_other_snapshots.elapsed().as_millis();

        // Clear current manifest.
        self.manifest_manager.take_current_manifest();
        self.vc_log_info.load_time_cost.all_time = start_load.elapsed().as_millis();
    }

    pub fn close(&mut self) {
        self.manifest_manager.close();
    }

    // Generate snapshot(version information file) such as manifests, memory index
    // manifests, dict snapshot, etc, and write them to local disk. Also record the
    // file metadata that may need to be uploaded.
    fn snapshot(
        &mut self,
        version: u64,
        seq_id: u64,
        csr_index: &CsrIndex,
        level_controller: &LevelController,
        upload_info: &mut FxHashSet<CopyFileMeta>,
        id_dict: &RockDbIdDict,
        label_dict: &LabelDict,
        max_registered_fid: &mut u32,
    ) {
        // Lock level_controller.
        let _lock_guard = level_controller.archive_write_guard();
        let start_snapshot_others = Instant::now();

        // Build snapshots other than manifest, including memory index, label dict, id
        // dict.
        let snapshots_info = ManifestManager::snapshot_except_manifest(
            &self.vc_inner,
            version,
            csr_index,
            level_controller,
            id_dict,
            label_dict,
        );
        self.vc_log_info.archive_time_cost.snapshot_others_time =
            start_snapshot_others.elapsed().as_millis();

        // Update upload information of all the snapshots.
        ManifestManager::update_snapshot_upload_info(
            &self.vc_inner,
            version,
            snapshots_info.memory_index_fid,
            upload_info,
        );

        let start_snapshot_manifest = Instant::now();

        // Build snapshot of manifest and write it to local
        self.manifest_manager
            .snapshot_manifest(
                &self.vc_inner,
                version,
                seq_id,
                level_controller,
                snapshots_info,
                max_registered_fid,
            )
            .unwrap_or_else(|e| {
                error_and_panic!("{}", e);
            });

        self.vc_log_info.archive_time_cost.snapshot_manifest_time =
            start_snapshot_manifest.elapsed().as_millis();
    }

    fn check_current_manifest(&self) -> Result<()> {
        if self.manifest_manager.is_current_manifest_exist() {
            return Ok(());
        }
        Err(Error::ArchiveError("Current manifest is None".to_string()))
    }

    // 1. Update upload information of index files and data files to the set storing upload meta by
    //    comparing deference of last manifest and current manifest.
    // 2. Record the max fid of index files and data files in current manifest, which is used to
    //    limit fid range for dropped files in auto-drop strategy.
    // 3. return index and data files which should be reserved.
    fn build_upload_info(&self, upload_info: &mut FxHashSet<CopyFileMeta>) {
        self.check_current_manifest().unwrap_or_else(|e| {
            error_and_panic!("{}", e);
        });

        // Get the last and current version of the FidToTableType to compare their
        // filenames.
        let (last_file_meta_map, current_file_meta_map) = self.get_last_current_file_meta_map();

        // Compare filenames of the latest and current version and get index files and
        // data files to reserve and upload.
        for file_meta in current_file_meta_map {
            let fid = file_meta.0;
            let table_type = file_meta.1;

            let file_name = build_file_name(fid, table_type);

            if !last_file_meta_map.contains_key(&file_meta.0) {
                // TODO After add file_handle_type to deferent type table in table config, let
                // value of file_handle_type come from table config.
                let file_handle_type = self.vc_inner.file_handle_type;
                let persistent_file_handle_type = self.vc_inner.persistent_file_handle_type;
                let index_data_file_upload_meta = CopyFileMeta {
                    file_name: file_name.clone(),
                    src_file_handle_type: file_handle_type,
                    dest_file_handle_type: persistent_file_handle_type,
                };

                // Update upload information of index files and data files to the set storing
                // upload meta.
                upload_info.insert(index_data_file_upload_meta);
            }
        }
    }

    // Get the index files and data files information in the manifests of current
    // archiving process and the last, which is used to compare two versions of
    // manifest files to derive new index files and value files, which are used
    // to generate meta of file that needs to be uploaded and saved.
    fn get_last_current_file_meta_map(&self) -> (FidToTableType, FidToTableType) {
        // Get FidToTableType of files which have been archived in last version.
        let mut last_file_meta_map: FidToTableType = FxHashMap::default();

        if let Some(last_manifest) = &self.manifest_manager.get_last_manifest_ref() {
            // TODO After separate Edge and vertex, change TableType => manifest.table_type
            for last_level_manifest in last_manifest.level_manifest_vec.iter() {
                last_level_manifest
                    .table_info_manifest_map
                    .iter()
                    .map(|item| item.1)
                    .for_each(|table_info| {
                        last_file_meta_map.insert(
                            table_info.index_table_manifest.as_ref().unwrap().fid,
                            TableType::Is,
                        );

                        last_file_meta_map.insert(
                            table_info.data_table_manifest.as_ref().unwrap().fid,
                            TableType::Vs,
                        );
                    });
            }
        }

        // Get FidToTableType of files which would be archived in current version.
        let mut current_file_meta_map: FidToTableType = FxHashMap::default();

        if let Some(current_manifest) = &self.manifest_manager.get_current_manifest_ref() {
            for current_level_manifest in current_manifest.level_manifest_vec.iter() {
                current_level_manifest
                    .table_info_manifest_map
                    .iter()
                    .map(|item| item.1)
                    .for_each(|table_info| {
                        current_file_meta_map.insert(
                            table_info.index_table_manifest.as_ref().unwrap().fid,
                            TableType::Is,
                        );

                        current_file_meta_map.insert(
                            table_info.data_table_manifest.as_ref().unwrap().fid,
                            TableType::Vs,
                        );
                    });
            }
        }

        (last_file_meta_map, current_file_meta_map)
    }

    fn drop_after_archive(
        &mut self,
        max_registered_fid: u32,
        cache_handler: &CStoreCacheHandler,
        level_controller: &LevelController,
        compact_drop_lock: &RwLock<()>,
    ) {
        let start_drop_persistent = Instant::now();
        // Drop files in persistent name space after completing an archive.
        ManifestDropStrategy::drop_persistent_name_space_after_archive(
            &self.vc_inner,
            cache_handler,
            self.manifest_manager.get_last_manifest_ref(),
            self.manifest_manager.get_current_manifest_ref(),
            level_controller,
            compact_drop_lock,
        );
        self.vc_log_info.archive_time_cost.drop_persistent_time =
            start_drop_persistent.elapsed().as_millis();

        let start_drop_local = Instant::now();
        // Drop files in local name space after completing an archive.
        ManifestDropStrategy::drop_local_name_space_after_archive(&self.vc_inner);
        self.vc_log_info.archive_time_cost.drop_local_time = start_drop_local.elapsed().as_millis();
    }

    fn check_archived(&mut self, version: u64) -> Result<()> {
        // Get the version from memory firstly.
        if let Some(manifest) = &self.manifest_manager.get_last_manifest_ref() {
            if manifest.version < version {
                return Ok(());
            }
        };

        // Get Version from memory First.
        let (last_version, last_last_version) = self.get_last_two_version()?;

        return if version < last_version {
            Err(Error::ArchiveError(format!(
                "Manifest version {} is lower than the current latest version {}. Fail to archive.",
                version, last_version
            )))
        } else {
            // Load latest manifest into memory
            if version > last_version && last_version != 0 {
                let last_manifest = ManifestManager::build_manifest(&self.vc_inner, last_version)?;
                self.manifest_manager.update_last_manifest(last_manifest);
            } else if version == last_version && last_last_version != 0 {
                let last_manifest =
                    ManifestManager::build_manifest(&self.vc_inner, last_last_version)?;

                self.manifest_manager.update_last_manifest(last_manifest);
            }

            Ok(())
        };

        Ok(())
    }

    fn check_loaded(&mut self, version: u64) -> Result<()> {
        let (last_version, _last_last_version) = self.get_last_two_version()?;

        // Check the legality of the version.
        if last_version > 0 {
            if version > last_version {
                return Err(Error::RecoverError(format!(
                    "Illegal version {}, latest version {}",
                    version, last_version
                )));
            }

            let last_manifest = ManifestManager::build_manifest(&self.vc_inner, last_version)?;
            self.manifest_manager.update_last_manifest(last_manifest);
        } else {
            return Err(Error::RecoverError("Never archived".to_string()));
        }

        // Check if version exists.
        let manifest_fid_set: FxHashSet<u64> = self
            .get_manifest_file_set()?
            .unwrap_or_default()
            .iter()
            .map(|file_name| {
                let version: u64 =
                    get_integer_from_str_suffix(file_name, MANIFEST_FILE_NAME_DELIMITER).unwrap();
                version
            })
            .collect();

        if !manifest_fid_set.contains(&version) {
            return Err(Error::RecoverError(format!(
                "Version {} is not exist.",
                version
            )));
        }

        Ok(())
    }

    // Get latest version from memory or disk.
    fn get_last_two_version(&mut self) -> Result<(u64, u64)> {
        // Get the version from disk.
        // List Manifest in persistent namespace and find the latest manifest.
        let mut version_set_iter = self
            .get_manifest_file_set()?
            .unwrap_or_default()
            .into_iter()
            .map(|file_name| {
                let version: u64 =
                    get_integer_from_str_suffix(&file_name, MANIFEST_FILE_NAME_DELIMITER).unwrap();
                version
            });

        let (last_version, last_last_version) =
            version_set_iter.fold((0u64, 0), |(max1, max2), num| {
                if num > max1 {
                    (num, max1)
                } else if num > max2 {
                    (max1, num)
                } else {
                    (max1, max2)
                }
            });

        Ok((last_version, last_last_version))
    }

    fn get_manifest_file_set(&self) -> Result<Option<FxHashSet<String>>> {
        let persistent_file_handle_type = &self.vc_inner.persistent_file_handle_type;

        let mut file_set = self.vc_inner.file_operator.list(
            persistent_file_handle_type,
            Path::new(&self.vc_inner.persistent_name_space),
            false,
        )?;

        file_set.retain(|item| item.contains(MANIFEST_FILE_NAME_PREFIX));

        if file_set.is_empty() {
            return Ok(None);
        }

        Ok(Some(file_set))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        engine::store_engine::ActionTrait,
        test_util,
        test_util::{bind_job_name_with_ts, delete_fo_test_dir},
        util::time_util::time_now_as_us,
        CStoreConfigBuilder, ConfigMap, Engine,
    };

    fn test_fo(config_map: &ConfigMap) {
        let cstore_config = CStoreConfigBuilder::default()
            .set_with_map(config_map)
            .build();
        let manifest_config = &cstore_config.manifest.clone();
        let persistent_config = &cstore_config.persistent.clone();

        let mut engine = Engine::new(cstore_config);

        for i in 0..10 {
            engine.archive(10);
        }

        engine.archive(30);
        engine.archive(40);

        engine.recover(40);
        engine.close();
        engine.recover(30);
        engine.close();

        delete_fo_test_dir(&manifest_config, &persistent_config);
    }
}
