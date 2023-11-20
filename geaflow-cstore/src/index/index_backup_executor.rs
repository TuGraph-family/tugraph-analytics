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

use std::path::{Path, PathBuf};

use itertools::Itertools;
use prost::Message;

use super::{csr_index::CsrIndex, parent_node::IndexMeta};
use crate::{
    convert_u64_to_usize,
    file::file_handle::{LocalBufferReader, LocalBufferWriter},
    gen::memory_index_manifest::{IndexMetaManifest, MemoryIndexManifest, ParentNodeManifest},
    lsm::level_controller::LevelController,
    ParentNode, Result, TableType,
};

pub struct CsrIndexBackupExecutor {}

impl CsrIndexBackupExecutor {
    pub fn snapshot(
        csr_index: &CsrIndex,
        level_controller: &LevelController,
        name_space: &str,
    ) -> Result<u32> {
        let mut memory_index_manifest = MemoryIndexManifest::default();

        {
            let index_modifier = csr_index.create_index_modifier();

            for item in index_modifier.iter() {
                memory_index_manifest.mem_index.insert(
                    *item.key(),
                    ParentNodeManifest {
                        index_meta_vec: item
                            .value()
                            .index_meta_vec
                            .iter()
                            .map(|index_meta| IndexMetaManifest {
                                fid: index_meta.fid,
                                offset: index_meta.offset,
                            })
                            .collect_vec(),
                    },
                );
            }
        }

        let fid = level_controller.get_next_fid();

        let file_path = PathBuf::new()
            .join(name_space)
            .join(fid.to_string())
            .with_extension(TableType::Mi.to_string().to_ascii_lowercase());

        let mut local_writer = LocalBufferWriter::new(&file_path)?;

        let mut memory_index_manifest_buf = Vec::with_capacity(memory_index_manifest.encoded_len());

        memory_index_manifest.encode(&mut memory_index_manifest_buf)?;

        local_writer.write(&memory_index_manifest_buf)?;
        local_writer.flush()?;

        Ok(fid)
    }

    pub fn load(memory_index_manifest_path: &Path, csr_index: &CsrIndex) -> Result<()> {
        let mut local_reader = LocalBufferReader::new(memory_index_manifest_path)?;
        let mut memory_index_manifest_buf =
            vec![0u8; convert_u64_to_usize!(local_reader.get_file_len())];
        local_reader.read(&mut memory_index_manifest_buf)?;

        let memory_index_manifest: MemoryIndexManifest =
            MemoryIndexManifest::decode(memory_index_manifest_buf.as_ref())?;

        {
            let index_modifier = csr_index.create_index_modifier();

            for item in memory_index_manifest.mem_index.iter() {
                index_modifier.insert(
                    *item.0,
                    ParentNode {
                        index_meta_vec: item
                            .1
                            .index_meta_vec
                            .iter()
                            .map(|index_table_manifest| IndexMeta {
                                fid: index_table_manifest.fid,
                                offset: index_table_manifest.offset,
                            })
                            .collect_vec(),
                    },
                );
            }
        }

        Ok(())
    }
}
