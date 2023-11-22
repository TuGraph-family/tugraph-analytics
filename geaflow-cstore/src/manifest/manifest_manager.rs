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
    collections::HashMap,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

use prost::Message;
use rustc_hash::FxHashSet;
use tracing::{span, Level, Span};

use super::{CopyFileMeta, VersionControllerInner};
use crate::{
    convert_u64_to_usize,
    engine::{
        dict::{
            id_dict::{IdDict, RockDbIdDict},
            label_dict::LabelDict,
            DictUtil,
        },
        sequence_id::SequenceId,
    },
    error_and_panic,
    file::{
        file_handle::{FileHandleType, LocalBufferReader, LocalBufferWriter},
        file_operator::{FileOperator, LocalFileOperator},
    },
    gen::manifest::{
        DataTableInfoManifest, IndexTableInfoManifest, LevelManifest, Manifest, TableInfoManifest,
    },
    index::{csr_index::CsrIndex, index_backup_executor::CsrIndexBackupExecutor},
    lsm::level_controller::LevelController,
    table::build_file_name,
    time_util::time_now_as_secs,
    Result, TableType,
};

// Map from fid of index file to manifest of the table info.
//
// It is used to build manifest file.
type TableInfoManifestMap = HashMap<u32, TableInfoManifest>;

// Information for build manifest.
#[derive(Debug, Default)]
pub struct SnapshotsInfo {
    pub memory_index_fid: u32,
    pub label_dict_snapshot_str: String,
}

// Information for loading snapshot files other than manifest.
// When loading the manifest, record the loading information (including file
// paths) of other snapshot files so that other snapshot files can be loaded
// later. There is no need to save the path manifest as the manifest will be
// loaded first.
#[derive(Debug, Default)]
pub struct LoadSnapshotsInfo {
    pub memory_index_manifest_path: PathBuf,
    pub id_dict_path: String,
    pub label_dict_snapshot_str: String,
}

// Parameters for backup executor begins.
//
// In manifest manager, there are some parallel execution tasks, including the
// snapshot generating and loading of memory index, dict, etc.
pub struct CsrIndexSnapshotParameters<'a> {
    pub csr_index: &'a CsrIndex,
    pub level_controller: &'a LevelController,
    pub vc_inner: &'a VersionControllerInner,
}

pub struct CsrIndexLoadParameters<'a> {
    pub csr_index: &'a CsrIndex,
    pub vc_inner: &'a VersionControllerInner,
    pub memory_index_manifest_path: PathBuf,
}

pub struct IdDictSnapshotParameters<'a> {
    pub vc_inner: &'a VersionControllerInner,
    pub id_dict: &'a RockDbIdDict,
}

pub struct IdDictLoadParameters<'a> {
    pub snapshot_path: String,
    pub id_dict: &'a RockDbIdDict,
}

pub struct LabelDictSnapshotParameters<'a> {
    pub label_dict: &'a LabelDict,
}

pub struct LabelDictLoadParameters<'a> {
    pub label_dict: &'a LabelDict,
    pub label_dict_snapshot_str: String,
}
// Parameters for backup executor ends.
#[derive(Debug, Default)]
pub struct ManifestManager {
    last_manifest: Option<Manifest>,

    current_manifest: Option<Manifest>,
}

impl ManifestManager {
    #[allow(clippy::too_many_arguments)]
    pub fn snapshot_manifest(
        &mut self,
        vc_inner: &VersionControllerInner,
        version: u64,
        seq_id: u64,
        level_controller: &LevelController,
        snapshots_info: SnapshotsInfo,
        max_registered_fid: &mut u32,
    ) -> Result<()> {
        let mut level_manifest_vec: Vec<LevelManifest> = Vec::new();

        // Archive TableInfoMap in all levels.
        for level in 0..level_controller.max_level() {
            // Archive TableInfoMap in cur level.
            let level_manifest_tmp = LevelManifest {
                table_info_manifest_map: Self::snapshot_one_level_table_info(
                    level_controller,
                    level,
                    max_registered_fid,
                ),
            };

            level_manifest_vec.push(level_manifest_tmp);
        }

        // Get the persistent path of the id dict snapshot, list the file paths of all
        // files under the path, record the path relative to the persistent namespace,
        // and use it to download the snapshot file of id dict.
        let id_dict_snapshot_path = DictUtil::get_snapshot_path(
            &vc_inner.persistent_name_space,
            &vc_inner.id_dict_key,
            version,
        );

        // Build Manifest.
        let current_manifest = Manifest {
            version,
            max_registered_fid: *max_registered_fid,
            memory_index_fid: snapshots_info.memory_index_fid,
            id_dict_snapshot_path,
            label_dict_snapshot_str: snapshots_info.label_dict_snapshot_str,
            create_ts: time_now_as_secs(),
            level_manifest_vec,
            seq_id,
        };

        // 2. Flush into the disk.
        let file_path = PathBuf::new()
            .join(&vc_inner.local_name_space)
            .join(build_file_name(version, TableType::Ms));

        let mut local_writer = LocalBufferWriter::new(&file_path)?;

        let mut manifest_buf = Vec::with_capacity(current_manifest.encoded_len());

        current_manifest.encode(&mut manifest_buf)?;

        local_writer.write(&manifest_buf)?;
        local_writer.flush()?;

        self.current_manifest = Some(current_manifest);

        Ok(())
    }

    // Build snapshots other than manifest, including memory index, label dict, id
    // dict. Execution of each task is parallel.
    pub fn snapshot_except_manifest(
        vc_inner: &VersionControllerInner,
        version: u64,
        csr_index: &CsrIndex,
        level_controller: &LevelController,
        id_dict: &RockDbIdDict,
        label_dict: &LabelDict,
    ) -> SnapshotsInfo {
        let memory_index_id = Arc::new(Mutex::new(0));
        let label_dict_snapshot_str = Arc::new(Mutex::new(String::new()));

        let memory_index_id_clone = Arc::clone(&memory_index_id);
        let label_dict_snapshot_str_clone = Arc::clone(&label_dict_snapshot_str);

        let child_span = span!(Level::DEBUG, "manifest.snapshot")
            .follows_from(Span::current().id())
            .clone()
            .or_current();
        vc_inner.thread_pool.scope(|s| {
            // Build snapshot of memory index and write it to local.
            let _span = child_span.entered();
            let csr_index_parameters = CsrIndexSnapshotParameters {
                csr_index,
                level_controller,
                vc_inner,
            };

            let csr_snapshot_span = span!(Level::DEBUG, "csr.snapshot")
                .follows_from(Span::current().id())
                .clone()
                .or_current();
            s.spawn(move |_| {
                let _span = csr_snapshot_span.entered();
                let memory_index_fid = CsrIndexBackupExecutor::snapshot(
                    csr_index_parameters.csr_index,
                    csr_index_parameters.level_controller,
                    &csr_index_parameters.vc_inner.local_name_space,
                )
                .unwrap_or_else(|e| {
                    error_and_panic!("{}", e);
                });

                let mut memory_index_id_lock = memory_index_id_clone.lock().unwrap();
                *memory_index_id_lock = memory_index_fid;
            });

            // Build snapshot of id dict and write it to local.
            let id_dict_parameters = IdDictSnapshotParameters { vc_inner, id_dict };

            let id_dict_snapshot_span = span!(Level::DEBUG, "id_dict.snapshot")
                .follows_from(Span::current().id())
                .clone()
                .or_current();
            s.spawn(move |_| {
                let _span = id_dict_snapshot_span.entered();
                let snapshot_path = DictUtil::get_snapshot_path(
                    &id_dict_parameters.vc_inner.local_name_space,
                    &id_dict_parameters.vc_inner.id_dict_key,
                    version,
                );

                id_dict_parameters.id_dict.snapshot(&snapshot_path);
            });

            // Build snapshot of label dict and write it to local.
            let label_dict_parameters = LabelDictSnapshotParameters { label_dict };

            let label_dict_snapshot_span = span!(Level::DEBUG, "label_dict.snapshot")
                .follows_from(Span::current().id())
                .clone()
                .or_current();
            s.spawn(move |_| {
                let _span = label_dict_snapshot_span.entered();
                let snapshot_str = label_dict_parameters.label_dict.snapshot();

                let mut label_dict_snapshot_str_lock =
                    label_dict_snapshot_str_clone.lock().unwrap();
                *label_dict_snapshot_str_lock = snapshot_str;
            });
        });

        let memory_index_fid: u32 = Arc::try_unwrap(memory_index_id)
            .unwrap()
            .into_inner()
            .unwrap();

        let label_dict_snapshot_str: String = Arc::try_unwrap(label_dict_snapshot_str)
            .unwrap()
            .into_inner()
            .unwrap();

        SnapshotsInfo {
            memory_index_fid,
            label_dict_snapshot_str,
        }
    }

    // Update upload information of all the snapshots.
    pub fn update_snapshot_upload_info(
        vc_inner: &VersionControllerInner,
        version: u64,
        memory_index_fid: u32,
        upload_info: &mut FxHashSet<CopyFileMeta>,
    ) {
        // Update the upload information of the id dict snapshot.
        Self::update_id_dict_snapshot_upload_info(vc_inner, version, upload_info);

        // Update the upload information of the memory index manifest.
        Self::update_memory_index_manifest_upload_info(vc_inner, memory_index_fid, upload_info);

        // Update the upload information of the manifest.
        Self::update_manifest_upload_info(vc_inner, version, upload_info);
    }

    // Update the upload information of the id dict snapshot.
    pub fn update_id_dict_snapshot_upload_info(
        vc_inner: &VersionControllerInner,
        version: u64,
        upload_info: &mut FxHashSet<CopyFileMeta>,
    ) {
        // Get the local path of the id dict snapshot, list the file paths of all files
        // under the path, record the path relative to the local namespace, and use it
        // to upload the snapshot file of id dict.
        let id_dict_snapshot_path =
            DictUtil::get_snapshot_path(&vc_inner.local_name_space, &vc_inner.id_dict_key, version);

        let id_dict_snapshot_path = Path::new(&id_dict_snapshot_path);
        let local_name_space_path = Path::new(&vc_inner.local_name_space);

        // The path of the snapshot directory of id dict relative to the local
        // namespace.
        let relative_id_dict_path = id_dict_snapshot_path
            .strip_prefix(local_name_space_path)
            .unwrap();

        let file_set = LocalFileOperator
            .list(id_dict_snapshot_path, true)
            .unwrap_or_else(|e| error_and_panic!("{}", e));

        for file_path in file_set {
            // Get the path of id dict snapshot file relative to the local namespace.
            let relative_id_dict_file_path = PathBuf::new()
                .join(relative_id_dict_path)
                .join(file_path)
                .to_str()
                .unwrap()
                .to_string();

            let id_dict_file_upload_meta = CopyFileMeta {
                file_name: relative_id_dict_file_path.clone(),
                src_file_handle_type: FileHandleType::default(),
                dest_file_handle_type: vc_inner.persistent_file_handle_type,
            };

            upload_info.insert(id_dict_file_upload_meta);
        }
    }

    pub fn load_manifest_snapshot(
        &mut self,
        vc_inner: &VersionControllerInner,
        version: u64,
        level_controller: &LevelController,
        sequence_id_container: &SequenceId,
    ) -> (FxHashSet<CopyFileMeta>, LoadSnapshotsInfo) {
        let file_handle_type = vc_inner.file_handle_type;

        let memory_index_fid: u32;
        let label_dict_snapshot_str: String;
        let mut download_info: FxHashSet<CopyFileMeta>;
        let id_dict_snapshot_path: String;

        {
            let persistent_file_handle_type = vc_inner.persistent_file_handle_type;
            // Load and get manifest.
            let manifest = self.load_manifest(vc_inner, version);

            // Load the level controller of lsm tree, initialize the tables in each level,
            // and record the information for downloading.
            download_info =
                level_controller.load(manifest, &file_handle_type, &persistent_file_handle_type);

            // recover sequence id.
            sequence_id_container.recover(manifest);

            memory_index_fid = manifest.memory_index_fid;
            label_dict_snapshot_str = manifest.label_dict_snapshot_str.clone();
            id_dict_snapshot_path = manifest.id_dict_snapshot_path.clone();
        }

        // Updated download and loading information for all snapshots.
        let load_snapshots_info = Self::update_snapshot_download_and_loading_info(
            vc_inner,
            version,
            &mut download_info,
            memory_index_fid,
            label_dict_snapshot_str,
            id_dict_snapshot_path,
        );

        (download_info, load_snapshots_info)
    }

    // Load snapshots other than manifest, including memory index, label dict, id
    // dict. Execution of each task is parallel.
    pub fn load_other_snapshot(
        vc_inner: &VersionControllerInner,
        csr_index: &CsrIndex,
        load_snapshots_info: &LoadSnapshotsInfo,
        id_dict: &RockDbIdDict,
        label_dict: &LabelDict,
    ) {
        let child_span = span!(Level::DEBUG, "manifest.other_snapshot")
            .follows_from(Span::current().id())
            .clone()
            .or_current();
        vc_inner.thread_pool.scope(|s| {
            let _span = child_span.entered();
            // Load manifest of memory index.
            let csr_index_parameters = CsrIndexLoadParameters {
                csr_index,
                vc_inner,
                memory_index_manifest_path: load_snapshots_info.memory_index_manifest_path.clone(),
            };

            let csr_load_span = span!(Level::DEBUG, "csr.load")
                .follows_from(Span::current().id())
                .clone()
                .or_current();
            s.spawn(move |_| {
                let _span = csr_load_span.entered();
                CsrIndexBackupExecutor::load(
                    &csr_index_parameters.memory_index_manifest_path,
                    csr_index_parameters.csr_index,
                )
                .unwrap();
            });

            // Load snapshot of id dict.
            let id_dict_parameters = IdDictLoadParameters {
                snapshot_path: load_snapshots_info.id_dict_path.clone(),
                id_dict,
            };

            let id_dict_recover_span = span!(Level::DEBUG, "id_dict.recover")
                .follows_from(Span::current().id())
                .clone()
                .or_current();
            s.spawn(move |_| {
                let _span = id_dict_recover_span.entered();
                id_dict_parameters
                    .id_dict
                    .recover(&id_dict_parameters.snapshot_path)
            });

            // Load snapshot of label dict.
            let label_dict_parameters = LabelDictLoadParameters {
                label_dict,
                label_dict_snapshot_str: load_snapshots_info.label_dict_snapshot_str.clone(),
            };

            let label_dict_recover_span = span!(Level::DEBUG, "label_dict.recover")
                .follows_from(Span::current().id())
                .clone()
                .or_current();
            s.spawn(move |_| {
                let _span = label_dict_recover_span.entered();
                label_dict_parameters
                    .label_dict
                    .recover(&label_dict_parameters.label_dict_snapshot_str);
            });
        });
    }

    pub fn build_manifest(vc_inner: &VersionControllerInner, version: u64) -> Result<Manifest> {
        let file_name = build_file_name(version, TableType::Ms);

        let src_path = PathBuf::new()
            .join(&vc_inner.persistent_name_space)
            .join(&file_name);
        let dest_path = PathBuf::new()
            .join(&vc_inner.local_name_space)
            .join(&file_name);

        // Download Manifest.
        vc_inner.file_copy_handle.copy_single_file(
            &vc_inner.persistent_file_handle_type,
            &FileHandleType::default(),
            &src_path,
            &dest_path,
        )?;

        // Read and Decode Manifest.
        let mut local_reader = LocalBufferReader::new(&dest_path)?;
        let mut manifest_buf = vec![0u8; convert_u64_to_usize!(local_reader.get_file_len())];
        local_reader.read(&mut manifest_buf)?;

        let manifest: Manifest = Manifest::decode(manifest_buf.as_ref())?;

        Ok(manifest)
    }

    pub fn get_current_manifest_ref(&self) -> &Option<Manifest> {
        &self.current_manifest
    }

    pub fn get_last_manifest_ref(&self) -> &Option<Manifest> {
        &self.last_manifest
    }

    pub fn take_current_manifest(&mut self) -> Option<Manifest> {
        self.current_manifest.take()
    }

    pub fn update_last_manifest(&mut self, manifest: Manifest) {
        self.last_manifest = Some(manifest);
    }

    pub fn is_current_manifest_exist(&self) -> bool {
        self.current_manifest.is_some()
    }

    pub fn close(&mut self) {
        self.last_manifest = None;
        self.current_manifest = None;
    }

    // Updated download and loading information for all snapshots.
    fn update_snapshot_download_and_loading_info(
        vc_inner: &VersionControllerInner,
        version: u64,
        download_info: &mut FxHashSet<CopyFileMeta>,
        memory_index_fid: u32,
        label_dict_snapshot_str: String,
        id_dict_snapshot_path: String,
    ) -> LoadSnapshotsInfo {
        let mut load_snapshots_info = LoadSnapshotsInfo::default();

        // Update download and loading information of memory index manifest.
        Self::update_memory_index_manifest_download_and_loading_info(
            vc_inner,
            download_info,
            memory_index_fid,
            &mut load_snapshots_info,
        );

        // Update download and loading information of id dict snapshot.
        Self::update_id_dict_snapshot_download_and_loading_info(
            vc_inner,
            version,
            &id_dict_snapshot_path,
            download_info,
            &mut load_snapshots_info,
        );

        // Update loading information of label dict snapshot.
        load_snapshots_info.label_dict_snapshot_str = label_dict_snapshot_str;

        load_snapshots_info
    }

    // Update download information of memory index manifest.
    fn update_memory_index_manifest_download_and_loading_info(
        vc_inner: &VersionControllerInner,
        download_info: &mut FxHashSet<CopyFileMeta>,
        memory_index_fid: u32,
        load_snapshots_info: &mut LoadSnapshotsInfo,
    ) {
        // Manifest of memory index.
        let memory_index_file_name = build_file_name(memory_index_fid, TableType::Mi);
        load_snapshots_info.memory_index_manifest_path = PathBuf::new().join(format!(
            "{}/{}",
            &vc_inner.local_name_space, &memory_index_file_name
        ));

        let memory_index_copy_meta = CopyFileMeta {
            file_name: memory_index_file_name,
            src_file_handle_type: vc_inner.persistent_file_handle_type,
            dest_file_handle_type: FileHandleType::default(),
        };
        download_info.insert(memory_index_copy_meta);
    }

    // Update download information of id dict snapshot.
    fn update_id_dict_snapshot_download_and_loading_info(
        vc_inner: &VersionControllerInner,
        version: u64,
        id_dict_snapshot_path: &String,
        download_info: &mut FxHashSet<CopyFileMeta>,
        load_snapshots_info: &mut LoadSnapshotsInfo,
    ) {
        // Record the local path of the id dict snapshot for recovery of id dict.
        load_snapshots_info.id_dict_path =
            DictUtil::get_snapshot_path(&vc_inner.local_name_space, &vc_inner.id_dict_key, version);

        let id_dict_snapshot_path = Path::new(id_dict_snapshot_path);
        let persistent_name_space = Path::new(&vc_inner.persistent_name_space);

        // The path of the snapshot directory of id dict relative to the persistent
        // namespace.
        let relative_id_dict_path = id_dict_snapshot_path
            .strip_prefix(persistent_name_space)
            .unwrap();

        let file_set = &vc_inner
            .file_operator
            .list(
                &vc_inner.persistent_file_handle_type,
                id_dict_snapshot_path,
                true,
            )
            .unwrap_or_else(|e| error_and_panic!("{}", e));

        for file_path in file_set {
            // Get the path of id dict snapshot file relative to the persistent namespace.
            let relative_id_dict_file_path = PathBuf::new()
                .join(relative_id_dict_path)
                .join(file_path)
                .to_str()
                .unwrap()
                .to_string();

            let relative_id_dict_file_copy_meta = CopyFileMeta {
                file_name: relative_id_dict_file_path.clone(),
                src_file_handle_type: vc_inner.persistent_file_handle_type,
                dest_file_handle_type: FileHandleType::default(),
            };

            download_info.insert(relative_id_dict_file_copy_meta);
        }
    }

    fn load_manifest(&mut self, vc_inner: &VersionControllerInner, version: u64) -> &Manifest {
        // Load Manifest.
        let manifest = if self.last_manifest.as_ref().unwrap().version == version {
            self.last_manifest.as_ref().unwrap()
        } else {
            let manifest = Self::build_manifest(vc_inner, version).unwrap_or_else(|e| {
                error_and_panic!("{}", e);
            });

            self.current_manifest = Some(manifest);
            self.current_manifest.as_ref().unwrap()
        };
        manifest
    }

    // Update the upload information of the memory index manifest.
    fn update_memory_index_manifest_upload_info(
        vc_inner: &VersionControllerInner,
        memory_index_fid: u32,
        upload_info: &mut FxHashSet<CopyFileMeta>,
    ) {
        let memory_index_file_name = build_file_name(memory_index_fid, TableType::Mi);

        let memory_index_upload_meta = CopyFileMeta {
            file_name: memory_index_file_name,
            src_file_handle_type: FileHandleType::default(),
            dest_file_handle_type: vc_inner.persistent_file_handle_type,
        };

        upload_info.insert(memory_index_upload_meta);
    }

    // Update the upload information of the manifest.
    fn update_manifest_upload_info(
        vc_inner: &VersionControllerInner,
        version: u64,
        upload_info: &mut FxHashSet<CopyFileMeta>,
    ) {
        let manifest_name = build_file_name(version, TableType::Ms);
        let manifest_upload_meta = CopyFileMeta {
            file_name: manifest_name,
            src_file_handle_type: FileHandleType::default(),
            dest_file_handle_type: vc_inner.persistent_file_handle_type,
        };
        upload_info.insert(manifest_upload_meta);
    }

    fn snapshot_one_level_table_info(
        level_controller: &LevelController,
        level: usize,
        max_registered_fid: &mut u32,
    ) -> TableInfoManifestMap {
        let mut table_info_manifest_map = HashMap::default();

        let level_read_lock_guard = level_controller.get_level_handler(level).read().unwrap();
        {
            level_read_lock_guard
                .get_table_info_map()
                .iter()
                .map(|iter| iter.1)
                .for_each(|table_info| {
                    let index_table_fid = table_info.index_table_info.fid;
                    let data_table_fid = table_info.data_table_info.fid;

                    *max_registered_fid = u32::max(*max_registered_fid, index_table_fid);
                    *max_registered_fid = u32::max(*max_registered_fid, data_table_fid);

                    // Insert TableInfo
                    table_info_manifest_map.insert(
                        // Key: index_table_fid
                        index_table_fid,
                        // TableInfo: (index_table_info, data_table_info)
                        TableInfoManifest {
                            // IndexTableInfo[size, create_ts, start_key, end_key]
                            index_table_manifest: Some(IndexTableInfoManifest {
                                fid: table_info.index_table_info.fid,
                                size: table_info.index_table_info.size,
                                uncompressed_size: table_info.index_table_info.uncompressed_size,
                                create_ts: table_info.index_table_info.create_ts,
                                start: table_info.index_table_info.start,
                                end: table_info.index_table_info.end,
                            }),

                            // DataTableInfo[fid, size]
                            data_table_manifest: Some(DataTableInfoManifest {
                                fid: data_table_fid,
                                size: table_info.data_table_info.size,
                            }),
                        },
                    );
                });
        }

        table_info_manifest_map
    }
}
