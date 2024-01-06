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

#![allow(clippy::new_without_default)]

use std::{
    collections::HashMap,
    ops::{Deref, DerefMut},
    path::PathBuf,
    str::FromStr,
    sync::Arc,
};

use config::{builder::DefaultState, Config, ConfigBuilder, Value};
use rustc_hash::FxHashMap;
use serde_derive::Deserialize;
use strum_macros::{Display, EnumString};

use self::cstore_config::CSTORE_CONFIG;
use crate::{
    file::file_handle::FileHandleType,
    log_util::{LogLevel, LogType},
    metric_recorder::ReporterType,
    serialize_util::SerializeType,
    CompressType,
};

pub mod cstore_config;

#[derive(Default, PartialEq, Debug, Clone)]
pub struct ConfigMap<'a>(FxHashMap<&'a str, &'a str>);

#[derive(Clone, Copy, Display, EnumString, PartialEq, Debug, Eq)]
#[strum(ascii_case_insensitive)]
enum LocationType {
    Local = 0,
    Remote = 1,
}

#[derive(Clone, Copy, Display, EnumString, PartialEq, Debug, Eq)]
#[strum(ascii_case_insensitive)]
pub enum PersistentType {
    Local = 0,
    #[cfg(feature = "opendal")]
    Oss,
    #[cfg(feature = "hdfs")]
    Hdfs,
}

impl<'a> FromIterator<(&'a str, &'a str)> for ConfigMap<'a> {
    fn from_iter<I: IntoIterator<Item = (&'a str, &'a str)>>(iter: I) -> Self {
        let mut map = FxHashMap::default();

        for (key, value) in iter {
            map.insert(key, value);
        }

        ConfigMap(map)
    }
}

impl<'a> FromIterator<(&'a String, &'a String)> for ConfigMap<'a> {
    fn from_iter<I: IntoIterator<Item = (&'a String, &'a String)>>(iter: I) -> Self {
        let mut map: FxHashMap<&str, &str> = FxHashMap::default();

        for (key, value) in iter {
            map.insert(key, value);
        }

        ConfigMap(map)
    }
}

impl<'a> From<Vec<(&'a str, &'a str)>> for ConfigMap<'a> {
    fn from(vec: Vec<(&'a str, &'a str)>) -> Self {
        let mut map = FxHashMap::default();
        for (key, value) in vec {
            map.insert(key, value);
        }
        ConfigMap(map)
    }
}

impl<'a> Deref for ConfigMap<'a> {
    type Target = FxHashMap<&'a str, &'a str>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<'a> DerefMut for ConfigMap<'a> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

/// Config structure used in cstore, which supports concurrency safety.
#[derive(Clone)]
pub struct CStoreConfig {
    pub store: Arc<StoreConfig>,
    pub index: IndexConfig,
    pub table: Arc<TableConfig>,
    pub cache: CacheConfig,
    pub dict: DictConfig,
    pub manifest: ManifestConfig,
    pub metric: MetricConfig,
    pub base: Arc<BaseConfig>,
    pub persistent: PersistentConfig,
    pub local: LocalConfig,
    pub log: LogConfig,
}

impl CStoreConfig {
    pub fn new(cstore_config_origin: CStoreConfigOrigin) -> Self {
        let base = Arc::new(BaseConfig::new(&cstore_config_origin));

        let store = StoreConfig::new(cstore_config_origin.store, &base);

        let index = IndexConfig::new(cstore_config_origin.index, &base);
        let table = TableConfig::new(cstore_config_origin.table, &base);
        let cache = CacheConfig::new(cstore_config_origin.cache, &base);
        let dict = DictConfig::new(cstore_config_origin.dict, &base);
        let manifest = ManifestConfig::new(cstore_config_origin.manifest, &base);
        let metric = MetricConfig::new(cstore_config_origin.metric, &base);
        let persistent = PersistentConfig::new(cstore_config_origin.persistent, &base);
        let local = LocalConfig::new(cstore_config_origin.local, &base);
        let log = LogConfig::new(cstore_config_origin.log, &base);

        CStoreConfig {
            store: Arc::new(store),
            index,
            table: Arc::new(table),
            dict,
            cache,
            manifest,
            metric,
            base,
            persistent,
            local,
            log,
        }
    }
}

impl Deref for CStoreConfig {
    type Target = Arc<BaseConfig>;

    fn deref(&self) -> &Self::Target {
        &self.base
    }
}

#[derive(Debug, Clone)]
pub struct BaseConfig {
    pub local_name_space: String,
    pub persistent_name_space: String,
    pub file_handle_type: FileHandleType,
    pub persistent_file_handle_type: FileHandleType,
    pub job_name: String,
    pub shard_index: u32,
    pub id_dict_key: String,
}

impl BaseConfig {
    pub fn new(cstore_config_origin: &CStoreConfigOrigin) -> Self {
        let store_config_origin = &cstore_config_origin.store;
        let local_config_origin = &cstore_config_origin.local;
        let persistent_config_origin = &cstore_config_origin.persistent;

        let persistent_name_space = concat_persistent_name_space(cstore_config_origin);
        let local_name_space = concat_local_name_space(cstore_config_origin);

        let persistent_type =
            PersistentType::from_str(&persistent_config_origin.persistent_type).unwrap();
        let location_type = LocationType::from_str(&store_config_origin.location).unwrap();

        let file_handle_type;
        let persistent_file_handle_type;

        if location_type == LocationType::Remote {
            if persistent_type != PersistentType::Local {
                file_handle_type =
                    FileHandleType::from_str(&persistent_config_origin.persistent_type).unwrap();
                persistent_file_handle_type =
                    FileHandleType::from_str(&persistent_config_origin.persistent_type).unwrap();
            } else {
                let persistent_file_handle_type_str =
                    format!("{}(Remote)", local_config_origin.file_handle_type);

                file_handle_type =
                    FileHandleType::from_str(&persistent_file_handle_type_str).unwrap();
                persistent_file_handle_type =
                    FileHandleType::from_str(&persistent_file_handle_type_str).unwrap();
            }
        } else {
            let file_handle_type_str = format!("{}(Local)", local_config_origin.file_handle_type);
            file_handle_type = FileHandleType::from_str(&file_handle_type_str).unwrap();

            if persistent_type == PersistentType::Local {
                let persistent_file_handle_type_str =
                    format!("{}(Remote)", local_config_origin.file_handle_type);
                persistent_file_handle_type =
                    FileHandleType::from_str(&persistent_file_handle_type_str).unwrap();
            } else {
                persistent_file_handle_type =
                    FileHandleType::from_str(&persistent_config_origin.persistent_type).unwrap();
            }
        }

        let job_name = cstore_config_origin.store.job_name.clone();
        let id_dict_key = cstore_config_origin.dict.id_dict_key.clone();

        Self {
            local_name_space,
            persistent_name_space,
            file_handle_type,
            persistent_file_handle_type,
            job_name,
            shard_index: cstore_config_origin.store.shard_index,
            id_dict_key,
        }
    }
}

#[derive(Debug, Clone)]
pub struct StoreConfig {
    store_config_origin: StoreConfigOrigin,
    pub base: Arc<BaseConfig>,
    pub serialize_type: SerializeType,
}

impl StoreConfig {
    pub fn new(store_config_origin: StoreConfigOrigin, base: &Arc<BaseConfig>) -> Self {
        let base = Arc::clone(base);
        let serialize_type: SerializeType =
            SerializeType::from_str(&store_config_origin.serialize_type).unwrap();

        StoreConfig {
            store_config_origin,
            base,
            serialize_type,
        }
    }
}

#[derive(Debug, Clone)]
pub struct IndexConfig {
    index_config_origin: IndexConfigOrigin,
    pub base: Arc<BaseConfig>,
}

impl IndexConfig {
    pub fn new(index_config_origin: IndexConfigOrigin, base: &Arc<BaseConfig>) -> Self {
        let base = Arc::clone(base);

        IndexConfig {
            index_config_origin,
            base,
        }
    }
}

#[derive(Debug, Clone)]
pub struct TableConfig {
    table_config_origin: TableConfigOrigin,
    pub base: Arc<BaseConfig>,
    pub compress_type: CompressType,
}

impl TableConfig {
    pub fn new(table_config_origin: TableConfigOrigin, base: &Arc<BaseConfig>) -> Self {
        let base = Arc::clone(base);
        let compress_type: CompressType =
            CompressType::from_str(&table_config_origin.compress_type).unwrap();

        TableConfig {
            table_config_origin,
            base,
            compress_type,
        }
    }
}

#[derive(Debug, Clone)]
pub struct CacheConfig {
    cache_config_origin: CacheConfigOrigin,
    pub base: Arc<BaseConfig>,
}

impl CacheConfig {
    pub fn new(cache_config_origin: CacheConfigOrigin, base: &Arc<BaseConfig>) -> Self {
        let base = Arc::clone(base);

        CacheConfig {
            cache_config_origin,
            base,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ManifestConfig {
    manifest_config_origin: ManifestConfigOrigin,
    pub base: Arc<BaseConfig>,
}

impl ManifestConfig {
    pub fn new(manifest_config_origin: ManifestConfigOrigin, base: &Arc<BaseConfig>) -> Self {
        let base = Arc::clone(base);

        ManifestConfig {
            manifest_config_origin,
            base,
        }
    }
}

#[derive(Debug, Clone)]
pub struct DictConfig {
    dict_config_origin: DictConfigOrigin,
    pub base: Arc<BaseConfig>,
}

impl DictConfig {
    pub fn new(dict_config_origin: DictConfigOrigin, base: &Arc<BaseConfig>) -> Self {
        let base = Arc::clone(base);

        DictConfig {
            dict_config_origin,
            base,
        }
    }
}

#[derive(Debug, Clone)]
pub struct LocalConfig {
    local_config_origin: LocalConfigOrigin,
    pub base: Arc<BaseConfig>,
}

impl LocalConfig {
    pub fn new(local_config_origin: LocalConfigOrigin, base: &Arc<BaseConfig>) -> Self {
        let base = Arc::clone(base);

        LocalConfig {
            local_config_origin,
            base,
        }
    }
}

#[derive(Debug, Clone)]
pub struct PersistentConfig {
    persistent_config_origin: PersistentConfigOrigin,
    pub base: Arc<BaseConfig>,
    pub persistent_type: PersistentType,
}

impl PersistentConfig {
    pub fn new(persistent_config_origin: PersistentConfigOrigin, base: &Arc<BaseConfig>) -> Self {
        let base = Arc::clone(base);
        let persistent_type =
            PersistentType::from_str(&persistent_config_origin.persistent_type).unwrap();

        PersistentConfig {
            persistent_config_origin,
            base,
            persistent_type,
        }
    }
}

#[derive(Debug, Clone)]
pub struct MetricConfig {
    metric_config_origin: MetricConfigOrigin,
    pub base: Arc<BaseConfig>,
    pub reporter_type: ReporterType,
}

impl MetricConfig {
    pub fn new(metric_config_origin: MetricConfigOrigin, base: &Arc<BaseConfig>) -> Self {
        let base = Arc::clone(base);
        let reporter_type =
            ReporterType::from_str(metric_config_origin.reporter_type.as_str()).unwrap();
        MetricConfig {
            metric_config_origin,
            base,
            reporter_type,
        }
    }
}

#[derive(Debug, Clone)]
pub struct LogConfig {
    log_config_origin: LogConfigOrigin,
    pub base: Arc<BaseConfig>,
    pub log_type: LogType,
    pub log_level: LogLevel,
}

impl LogConfig {
    pub fn new(log_config_origin: LogConfigOrigin, base: &Arc<BaseConfig>) -> Self {
        let base = Arc::clone(base);
        let log_type = LogType::from_str(&log_config_origin.log_type).unwrap_or_default();
        let log_level = LogLevel::from_str(&log_config_origin.log_level).unwrap_or_default();

        LogConfig {
            log_config_origin,
            base,
            log_type,
            log_level,
        }
    }
}

impl Deref for StoreConfig {
    type Target = StoreConfigOrigin;

    fn deref(&self) -> &Self::Target {
        &self.store_config_origin
    }
}

impl Deref for TableConfig {
    type Target = TableConfigOrigin;

    fn deref(&self) -> &Self::Target {
        &self.table_config_origin
    }
}

impl Deref for IndexConfig {
    type Target = IndexConfigOrigin;

    fn deref(&self) -> &Self::Target {
        &self.index_config_origin
    }
}

impl Deref for CacheConfig {
    type Target = CacheConfigOrigin;

    fn deref(&self) -> &Self::Target {
        &self.cache_config_origin
    }
}

impl Deref for DictConfig {
    type Target = DictConfigOrigin;

    fn deref(&self) -> &Self::Target {
        &self.dict_config_origin
    }
}

impl Deref for ManifestConfig {
    type Target = ManifestConfigOrigin;

    fn deref(&self) -> &Self::Target {
        &self.manifest_config_origin
    }
}

impl Deref for LocalConfig {
    type Target = LocalConfigOrigin;

    fn deref(&self) -> &Self::Target {
        &self.local_config_origin
    }
}

impl Deref for PersistentConfig {
    type Target = PersistentConfigOrigin;

    fn deref(&self) -> &Self::Target {
        &self.persistent_config_origin
    }
}

impl Deref for MetricConfig {
    type Target = MetricConfigOrigin;

    fn deref(&self) -> &Self::Target {
        &self.metric_config_origin
    }
}

impl Deref for LogConfig {
    type Target = LogConfigOrigin;

    fn deref(&self) -> &Self::Target {
        &self.log_config_origin
    }
}

/// Deserialize, Cloned config structure, which does not support concurrency
/// safety.
#[derive(Debug, Deserialize, Clone)]
pub struct CStoreConfigOrigin {
    pub table: TableConfigOrigin,
    pub store: StoreConfigOrigin,
    pub index: IndexConfigOrigin,
    pub cache: CacheConfigOrigin,
    pub dict: DictConfigOrigin,
    pub manifest: ManifestConfigOrigin,
    pub local: LocalConfigOrigin,
    pub persistent: PersistentConfigOrigin,
    pub metric: MetricConfigOrigin,
    pub log: LogConfigOrigin,
}

#[derive(Debug, Deserialize, Clone)]
pub struct CacheConfigOrigin {
    pub block_cache_item_number: usize,
    pub block_cache_capacity: u64,
    pub table_meta_cache_item_number: usize,
    pub table_meta_cache_capacity: u64,
    pub file_cache_item_number: usize,
    pub each_file_pool_capacity: usize,
    pub manifest_cache_number: usize,
}

#[derive(Debug, Deserialize, Clone)]
pub struct DictConfigOrigin {
    pub id_dict_key: String,
    pub id_dict_cache_ttl_secs: u64,
}

#[derive(Debug, Deserialize, Clone)]
pub struct TableConfigOrigin {
    pub block_size: u32,
    pub block_capacity_critical_ratio_percentage: u32,
    pub block_size_max_value: u32,

    pub compress_type: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct StoreConfigOrigin {
    pub location: String,
    pub job_name: String,
    pub store_name: String,
    pub shard_index: u32,
    pub mem_segment_size: usize,
    pub max_buffer_size_in_segment: usize,
    pub serialize_type: String,
    pub compact_thread_num: usize,
    pub compact_drop_thread_num: usize,
    pub compactor_interval: u64,
    pub compactor_gc_ratio: f64,
    pub compact_drop_multiple: u32,
    pub max_level: usize,
    pub level0_file_num: usize,
    pub level1_file_size: u64,
    pub file_size_multiplier: usize,
    pub io_thread_num: usize,
    pub sort_field: String,
    pub store_ttl: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct IndexConfigOrigin {
    pub index_granularity: u32,
    pub index_vector_len_bit_shift: u32,
}

#[derive(Debug, Deserialize, Clone)]
pub struct ManifestConfigOrigin {
    pub copy_buffer_size: usize,
    pub max_manifest_num: usize,
    pub multi_work_threads_num: usize,
}

#[derive(Debug, Deserialize, Clone)]
pub struct LocalConfigOrigin {
    pub file_handle_type: String,
    pub root: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct PersistentConfigOrigin {
    // Local Config
    pub persistent_type: String,
    pub root: String,
    pub config: Option<HashMap<String, String>>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct MetricConfigOrigin {
    pub reporter_type: String,
    pub enable_push_gateway: bool,
    pub reporter_interval_secs: u64,
    pub reporter_gw_endpoint: String,
    pub reporter_gw_username: String,
    pub reporter_gw_password: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct LogConfigOrigin {
    pub enable_log: bool,
    pub enable_trace: bool,
    pub log_type: String,
    pub log_level: String,
}

impl Default for CStoreConfigOrigin {
    fn default() -> Self {
        let config = Config::builder()
            .add_source(config::File::from_str(
                CSTORE_CONFIG,
                config::FileFormat::Json,
            ))
            .build()
            .unwrap();

        config.try_deserialize().unwrap()
    }
}

impl Default for CStoreConfig {
    fn default() -> Self {
        let cstore_config_origin: CStoreConfigOrigin = CStoreConfigOrigin::default();
        assert!(cstore_config_origin.manifest.max_manifest_num > 1);

        CStoreConfig::new(cstore_config_origin)
    }
}

pub struct CStoreConfigBuilder(Option<ConfigBuilder<DefaultState>>);

impl Deref for CStoreConfigBuilder {
    type Target = ConfigBuilder<DefaultState>;

    fn deref(&self) -> &Self::Target {
        self.0.as_ref().unwrap()
    }
}

impl Default for CStoreConfigBuilder {
    fn default() -> CStoreConfigBuilder {
        let config_builder: ConfigBuilder<DefaultState> = Config::builder().add_source(
            config::File::from_str(CSTORE_CONFIG, config::FileFormat::Json),
        );

        CStoreConfigBuilder(Some(config_builder))
    }
}

// CStoreConfigBuilder
impl CStoreConfigBuilder {
    pub fn set<T>(mut self, key: &str, value: T) -> CStoreConfigBuilder
    where
        T: Into<Value>,
    {
        self.0 = Some(self.0.take().unwrap().set_override(key, value).unwrap());
        self
    }

    pub fn set_with_map(mut self, config_map: &ConfigMap) -> CStoreConfigBuilder {
        config_map.iter().for_each(|config_pair| {
            self.0 = Some(
                self.0
                    .take()
                    .unwrap()
                    .set_override(*config_pair.0, *config_pair.1)
                    .unwrap(),
            );
        });

        self
    }

    pub fn build(&mut self) -> CStoreConfig {
        let cstore_config_origin: CStoreConfigOrigin = self
            .0
            .take()
            .unwrap()
            .build()
            .unwrap()
            .try_deserialize()
            .unwrap();

        CStoreConfig::new(cstore_config_origin)
    }
}

fn concat_local_name_space(config_origin: &CStoreConfigOrigin) -> String {
    let path = PathBuf::new()
        .join(config_origin.local.root.as_str())
        .join(config_origin.store.job_name.as_str())
        .join(config_origin.store.store_name.as_str())
        .join(config_origin.store.shard_index.to_string().as_str());
    path.to_str().unwrap().to_string()
}

fn concat_persistent_name_space(config_origin: &CStoreConfigOrigin) -> String {
    let path = PathBuf::new()
        .join(config_origin.persistent.root.as_str())
        .join(config_origin.store.job_name.as_str())
        .join(config_origin.store.store_name.as_str())
        .join(config_origin.store.shard_index.to_string().as_str());
    path.to_str().unwrap().to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default() {
        let config = CStoreConfig::default();
        assert_eq!(config.store.store_name, "graph");

        assert_eq!(config.file_handle_type, FileHandleType::default(),);
    }

    #[test]
    fn test_map() {
        // 1. Build ConfigMap by default and insert.
        let mut config_map_1 = ConfigMap::default();
        config_map_1.insert("test", "true");

        // 2. Build ConfigMap by Vec<(&str, &str)>.
        let config_map_2 = ConfigMap::from(vec![("test", "true")]);

        // 3. Build ConfigMap by IntoIter<(&str, &str)>.
        let config_map_3 = ConfigMap::from_iter([("test", "true")].into_iter());

        // 4. Build ConfigMap by.
        let mut source_map: FxHashMap<String, String> = FxHashMap::default();
        source_map.insert("test".to_string(), "true".to_string());
        let config_map_4 = ConfigMap::from_iter(source_map.iter());

        assert_eq!(config_map_1, config_map_2);
        assert_eq!(config_map_1, config_map_3);
        assert_eq!(config_map_1, config_map_4);
    }

    #[test]
    fn test_set_with_map() {
        let mut config_map = ConfigMap::default();
        config_map.insert("store.store_name", "graph1");

        let config = CStoreConfigBuilder::default()
            .set_with_map(&config_map)
            .build();
        assert_eq!(config.store.store_name, "graph1");
    }

    #[test]
    fn test_set() {
        let config = CStoreConfigBuilder::default()
            .set("store.store_name", "graph1")
            .build();

        assert_eq!(config.store.store_name, "graph1");
    }

    #[test]
    fn test_set_combine() {
        let mut config_map = ConfigMap::default();
        config_map.insert("store.store_name", "graph1");

        let config = CStoreConfigBuilder::default()
            .set("store.store_name", "graph2")
            .set_with_map(&config_map)
            .build();

        assert_eq!(config.store.store_name, "graph1");
    }

    #[test]
    fn test_persistent_config() {
        let mut config_map = ConfigMap::default();
        config_map.insert("persistent.persistent_type", "Local");
        config_map.insert("persistent.root", "/tmp/geaflow_cstore_local");

        let config1 = CStoreConfigBuilder::default()
            .set_with_map(&config_map)
            .build();
        assert!(&config1.persistent.config.is_none());
    }

    #[cfg(feature = "opendal")]
    #[test]
    fn test_oss_config() {
        let mut config_map = ConfigMap::default();
        config_map.insert("persistent.persistent_type", "Oss");
        config_map.insert("persistent.root", "/tmp/geaflow_cstore_remote");
        config_map.insert("persistent.config.endpoint", "");
        config_map.insert("persistent.config.bucket", "test");
        config_map.insert("persistent.access_key_id", "");
        config_map.insert("persistent.access_key_secret", "");
        let config2 = CStoreConfigBuilder::default()
            .set_with_map(&config_map)
            .build();
        let persistent_config = config2.persistent.clone();
        assert!(
            persistent_config
                .config
                .clone()
                .unwrap()
                .contains_key("endpoint")
        );
    }
}
