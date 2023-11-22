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
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex, RwLock,
    },
    thread,
    thread::JoinHandle,
    time::Duration,
};

use lazy_static::lazy_static;
use metrics::{register_gauge, Gauge};

use crate::config::MetricConfig;

pub mod metric_recorder;
mod prometheus_recorder;
mod simple_log_recorder;

lazy_static! {
    pub static ref BLOCK_CACHE_MEMORY: Gauge = register_gauge!("state_block_cache_memory");
    pub static ref TABLE_META_CACHE_MEMORY: Gauge = register_gauge!("state_table_meta_memory");
    pub static ref GRAPH_SEGMENT_MEMORY: Gauge = register_gauge!("state_graph_segment_memory");
    pub static ref CSR_INDEX_MAP_MEMORY: Gauge = register_gauge!("state_crs_index_map_memory");
    pub static ref ID_DICT_MAP_MEMORY: Gauge = register_gauge!("state_id_dict_map_memory");
}

pub trait MetricUpdater: Send + Sync {
    fn update(&self);
}

type MetricUpdaterType = RwLock<Vec<Arc<dyn MetricUpdater>>>;

pub struct MetricCollector {
    updaters: Arc<MetricUpdaterType>,
    join_handle_vec: Mutex<Vec<JoinHandle<()>>>,
    stop: Arc<AtomicBool>,
}

impl MetricCollector {
    pub fn new(config: &MetricConfig) -> Self {
        let interval = config.reporter_interval_secs;
        let updaters: Arc<MetricUpdaterType> = Arc::new(RwLock::new(Vec::new()));
        let c_updaters = Arc::clone(&updaters);
        let stop = Arc::new(AtomicBool::new(false));

        let stop_in_thread = Arc::clone(&stop);
        let thread_handle = thread::Builder::new()
            .name("metric_collector".to_string())
            .spawn(move || {
                while stop_in_thread.load(Ordering::SeqCst) {
                    {
                        let updaters = c_updaters.read().unwrap();
                        for updater in updaters.as_slice() {
                            updater.update();
                        }
                    }
                    thread::sleep(Duration::from_secs(interval))
                }
            })
            .unwrap();

        Self {
            updaters,
            stop,
            join_handle_vec: Mutex::new(vec![thread_handle]),
        }
    }

    pub fn register(&self, updater: Arc<dyn MetricUpdater>) {
        let mut updaters = self.updaters.write().unwrap();
        updaters.push(updater);
    }

    pub fn close(&self) {
        self.stop.store(true, Ordering::SeqCst);
        self.join_handle_vec
            .lock()
            .unwrap()
            .pop()
            .unwrap()
            .join()
            .unwrap();
    }
}
