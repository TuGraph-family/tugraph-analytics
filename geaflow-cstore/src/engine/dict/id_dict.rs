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
    os::raw::c_double,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU32, Ordering},
        Mutex, RwLock, RwLockReadGuard, RwLockWriteGuard,
    },
};

use libc::size_t;
use rocksdb::{
    checkpoint::Checkpoint, BlockBasedOptions, Cache, FlushOptions, IteratorMode, Options, DB,
};
use rustc_hash::FxHashMap;

use crate::{
    error_and_panic,
    file::file_operator::{FileOperator, LocalFileOperator},
    get_u32_from_bytes,
    log_util::info,
    metric,
    util::{
        number_util::{KIB, MIB},
        string_util::get_rfind_front_section,
    },
    FILE_PATH_DELIMITER,
};

const INITIAL_ID: u32 = 1;
const ID_BYTE: &[u8] = "COUNTER".as_bytes();

pub trait IdDict {
    // register key and return id;
    fn register(&self, key: &[u8]) -> u32;

    // get id from dict.
    fn get_id(&self, key: &[u8]) -> Option<u32>;

    // get origin key from reverse dict.
    fn get_origin_key(&self, id: u32) -> Option<Vec<u8>>;

    fn clear_reverse_dict(&self);

    // snapshot dict to given path.
    fn snapshot(&self, snapshot_path: &str);

    // recover from given path.
    fn recover(&self, recover_path: &str);

    fn drop_local(&self);
}

pub struct RockDbIdDict {
    id: AtomicU32,
    db: RwLock<Option<DB>>,
    file_path: String,
    reverse_dict: Mutex<FxHashMap<u32, Vec<u8>>>,
}

impl RockDbIdDict {
    pub fn new(local_name_space: &str, dict_key: &str) -> Self {
        let dir_path = PathBuf::new().join(local_name_space).join(dict_key);
        let dict_path_str = dir_path.to_str().unwrap();

        LocalFileOperator
            .remove_path(&dir_path)
            .unwrap_or_else(|e| {
                error_and_panic!("{}", e);
            });
        LocalFileOperator
            .create_dir(Path::new(local_name_space))
            .unwrap_or_else(|e| {
                error_and_panic!("{}", e);
            });

        info!("open id dict path {}", dict_path_str);
        let opts = RockDbIdDict::get_options();
        let db = DB::open(&opts, dict_path_str).unwrap();
        let db_opt = RwLock::new(Some(db));

        RockDbIdDict {
            id: AtomicU32::new(INITIAL_ID),
            db: db_opt,
            file_path: String::from(dict_path_str),
            reverse_dict: Mutex::new(FxHashMap::default()),
        }
    }

    pub fn get_options() -> Options {
        let mut cf_opts = Options::default();
        cf_opts.set_write_buffer_size((128 * MIB) as usize);
        cf_opts.set_max_write_buffer_number(2);
        cf_opts.create_if_missing(true);
        cf_opts.create_missing_column_families(true);
        cf_opts.set_use_direct_io_for_flush_and_compaction(true);

        let mut table_option = BlockBasedOptions::default();
        table_option.set_block_size((4 * KIB) as usize);
        table_option.set_bloom_filter(10 as c_double, false);
        table_option.set_cache_index_and_filter_blocks(true);
        table_option.set_pin_l0_filter_and_index_blocks_in_cache(true);
        let cache: Cache = Cache::new_lru_cache((512 * MIB) as size_t);
        table_option.set_block_cache(&cache);

        cf_opts.set_block_based_table_factory(&table_option);
        cf_opts
    }

    fn put_id(&self, key: &[u8], id: &u32) {
        let read_lock_guard = self.read_lock_guard();
        let db = read_lock_guard.as_ref().unwrap();

        db.put(key, id.to_be_bytes()).unwrap();
    }

    fn drop_db(&self) {
        let mut write_lock_guard = self.write_lock_guard();
        write_lock_guard.take();

        info!("drop id dict path {}", self.file_path.as_str());
        LocalFileOperator
            .remove_path(Path::new(self.file_path.as_str()))
            .unwrap_or_else(|e| {
                error_and_panic!("{}", e);
            });
    }

    fn read_lock_guard(&self) -> RwLockReadGuard<Option<DB>> {
        self.db.read().unwrap()
    }

    fn write_lock_guard(&self) -> RwLockWriteGuard<Option<DB>> {
        self.db.write().unwrap()
    }

    fn load_reverse_dict(&self) {
        let read_lock_guard = self.write_lock_guard();
        let db = read_lock_guard.as_ref().unwrap();
        let iter = db.iterator(IteratorMode::Start);

        let mut weight_size = 0;
        for item in iter {
            let (key, id) = item.unwrap();
            self.reverse_dict
                .lock()
                .unwrap()
                .insert(get_u32_from_bytes!(id.as_ref(), 0), key.as_ref().to_vec());
            weight_size += id.len() + key.len();
        }
        metric::ID_DICT_MAP_MEMORY.set(weight_size as f64);
    }
}

impl IdDict for RockDbIdDict {
    fn register(&self, key: &[u8]) -> u32 {
        assert!(!key.is_empty(), "key should not be empty.");
        let value = self.get_id(key);
        match value {
            Some(id) => id,
            None => {
                let next_id = self.id.fetch_add(1, Ordering::SeqCst);
                assert!(
                    next_id < u32::MAX,
                    "id should not greater than max value of u32"
                );
                self.put_id(key, &next_id);
                next_id
            }
        }
    }

    fn get_id(&self, key: &[u8]) -> Option<u32> {
        let read_lock_guard = self.read_lock_guard();
        let db = read_lock_guard.as_ref().unwrap();

        let val = db.get(key).unwrap();
        if let Some(id) = val {
            Option::from(get_u32_from_bytes!(id.as_slice(), 0))
        } else {
            None
        }
    }

    fn get_origin_key(&self, id: u32) -> Option<Vec<u8>> {
        if !self.reverse_dict.lock().unwrap().contains_key(&id) {
            self.load_reverse_dict();
        }
        if let Some(bytes) = self.reverse_dict.lock().unwrap().get(&id) {
            Option::from(bytes.to_vec())
        } else {
            None
        }
    }

    fn clear_reverse_dict(&self) {
        self.reverse_dict.lock().unwrap().clear();
    }

    fn snapshot(&self, chk_path: &str) {
        let read_lock_guard = self.read_lock_guard();
        let db = read_lock_guard.as_ref().unwrap();

        LocalFileOperator
            .remove_path(Path::new(chk_path))
            .unwrap_or_else(|e| {
                error_and_panic!("{}", e);
            });

        let parent_path = get_rfind_front_section(chk_path, FILE_PATH_DELIMITER);
        LocalFileOperator
            .create_dir(Path::new(parent_path))
            .unwrap_or_else(|e| {
                error_and_panic!("{}", e);
            });

        let current_id = self.id.load(Ordering::SeqCst);
        db.put(ID_BYTE, u32::to_be_bytes(current_id)).unwrap();
        db.flush_opt(&FlushOptions::default()).unwrap();
        info!("snapshot id dict with id {}: {}", current_id, chk_path);

        // TODO: support delta checkpoint.
        let checkpoint: Checkpoint = Checkpoint::new(db).unwrap();

        checkpoint.create_checkpoint(chk_path).unwrap();
    }

    fn recover(&self, chk_path: &str) {
        self.drop_db();

        let path: &Path = Path::new(self.file_path.as_str());
        LocalFileOperator
            .create_dir(path.parent().unwrap())
            .unwrap_or_else(|e| {
                error_and_panic!("{}", e);
            });
        LocalFileOperator
            .rename_dir(Path::new(chk_path), path)
            .unwrap_or_else(|e| {
                error_and_panic!("{}", e);
            });

        {
            let mut write_lock_guard = self.write_lock_guard();
            let opts = RockDbIdDict::get_options();
            let _ = write_lock_guard.insert(DB::open(&opts, &self.file_path).unwrap());
        }

        let current_id = self.get_id(ID_BYTE).unwrap();
        self.id.store(current_id, Ordering::SeqCst);
        info!("recovered id dict with id {}: {}", current_id, chk_path);
    }

    fn drop_local(&self) {
        self.drop_db();
    }
}

#[cfg(test)]
mod tests {
    use rand::{RngCore, SeedableRng};
    use rand_isaac::IsaacRng;

    use super::*;

    #[test]
    fn test_rocksdb_dict() {
        let dict_path = "/tmp/RocksDbDictTest";
        let chk_path = "/tmp/RocksDbDictTest/chk/1";

        LocalFileOperator.remove_path(Path::new(dict_path)).unwrap();
        LocalFileOperator.remove_path(Path::new(chk_path)).unwrap();

        let mut dict = RockDbIdDict::new(dict_path, "dict");
        gen_and_check_dict_data(&mut dict);

        dict.snapshot(chk_path);
        dict.recover(chk_path);
        assert_eq!(dict.get_id(ID_BYTE).unwrap(), 1000);
        assert!(!Path::new(chk_path).exists());

        let origin_key = dict.get_origin_key(100);
        assert!(origin_key.is_some());
    }

    fn gen_and_check_dict_data(dict: &mut dyn IdDict) {
        for i in 1..1000 {
            let mut key = vec![0; 1024];
            let mut rng: IsaacRng = SeedableRng::from_entropy();
            rng.fill_bytes(&mut key);

            let id1 = dict.register(key.as_slice());
            assert_eq!(id1, i);
            let id2 = dict.get_id(key.as_ref());
            assert!(id2.is_some());
            assert_eq!(id2.unwrap(), i);

            let origin_key = dict.get_origin_key(i);
            assert!(origin_key.is_some());
            assert_eq!(origin_key.unwrap(), key);
        }
    }
}
