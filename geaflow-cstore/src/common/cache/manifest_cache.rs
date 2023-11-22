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

use std::{path::PathBuf, sync::Arc};

use prost::Message;
use quick_cache::sync::Cache;

use crate::{
    config::CacheConfig,
    convert_u64_to_usize,
    error::Error,
    file::file_handle::{FileHandleType, LocalBufferReader},
    gen::manifest::Manifest,
    manifest::VersionControllerInner,
    table::build_file_name,
    Result, TableType,
};

// Manifest cache is only used to compare different manifests in the drop
// strategy after archiving.
pub struct ManifestCache {
    pub manifest_cache: Cache<u64, Arc<Manifest>>,

    pub manifest_cache_num: usize,
}

impl ManifestCache {
    pub fn new(cache_config: &CacheConfig) -> Self {
        let items_capacity = cache_config.manifest_cache_number;
        let cache: Cache<u64, Arc<Manifest>> = Cache::new(items_capacity);

        ManifestCache {
            manifest_cache: cache,
            manifest_cache_num: items_capacity,
        }
    }

    pub fn get_with(&self, key: &u64, parameter: &VersionControllerInner) -> Result<Arc<Manifest>> {
        self.manifest_cache
            .get_or_insert_with::<Error>(key, || Self::get_manifest(*key, parameter))
    }

    pub fn capacity(&self) -> usize {
        self.manifest_cache_num
    }

    // Estimate weight.
    pub fn weighted_size(&self) -> u64 {
        self.manifest_cache.weight()
    }

    pub fn get_manifest(version: u64, parameter: &VersionControllerInner) -> Result<Arc<Manifest>> {
        let file_name = build_file_name(version, TableType::Ms);

        let dest_path = PathBuf::new()
            .join(&parameter.local_name_space)
            .join(&file_name);

        // Since the manifest cache is only used to compare different manifests in the
        // drop strategy after archiving, the manifest stored locally can be trusted.
        if !dest_path.as_path().exists() {
            let src_path = PathBuf::new()
                .join(&parameter.persistent_name_space)
                .join(&file_name);

            // Download Manifest.
            parameter.file_copy_handle.copy_single_file(
                &parameter.persistent_file_handle_type,
                &FileHandleType::default(),
                &src_path,
                &dest_path,
            )?;
        }

        // Read and Decode Manifest.
        let mut local_reader = LocalBufferReader::new(&dest_path)?;

        let mut manifest_buf = vec![0u8; convert_u64_to_usize!(local_reader.get_file_len())];
        local_reader.read(&mut manifest_buf)?;

        let manifest: Manifest = Manifest::decode(manifest_buf.as_ref())?;

        Ok(Arc::new(manifest))
    }
}
