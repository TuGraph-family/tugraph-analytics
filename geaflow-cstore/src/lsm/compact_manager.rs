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
        atomic::{AtomicBool, AtomicU32, Ordering},
        mpsc::{self, Sender},
        Arc, RwLock,
    },
    thread,
    thread::JoinHandle,
    time,
};

use dashmap::DashMap;

use super::compact::compact_drop_strategy::CompactDropStrategy;
use crate::{
    config::{StoreConfig, TableConfig},
    error_and_panic,
    index::csr_index::CsrIndex,
    lsm::{
        compact::single_compactor::{CompactStatus, SingleCompactor},
        level_controller::LevelController,
    },
    s_info,
    util::thread_util::RayonThreadPool,
    CstoreFileOperator, EngineContext,
};

const INTERVAL_SECONDS_BETWEEN_OUTPUT_COMPACTION_LOG_INFO: u64 = 600;

struct CompactionExitPill<'a> {
    stop: &'a AtomicBool,
}

impl<'a> Drop for CompactionExitPill<'a> {
    fn drop(&mut self) {
        if !self.stop.load(Ordering::SeqCst) {
            self.stop.store(true, Ordering::SeqCst);
            error_and_panic!("Some error happen in compaction thread");
        }
    }
}

pub struct CompactManager {
    cpt_mng_inner: Arc<CompactManagerInner>,
    stop: Arc<AtomicBool>,
    join_handle_vec: Vec<(JoinHandle<()>, Sender<()>)>,

    compact_drop_thread_pool: Arc<RayonThreadPool>,
}

impl CompactManager {
    pub fn new(
        store_config: Arc<StoreConfig>,
        table_config: Arc<TableConfig>,
        engine_context: Arc<EngineContext>,
        csr_index: Arc<CsrIndex>,
        level_controller: Arc<LevelController>,
        file_operator: Arc<CstoreFileOperator>,
    ) -> Self {
        let compact_thread_num = store_config.compact_thread_num;
        let compact_drop_thread_num = store_config.compact_drop_thread_num;
        CompactManager {
            cpt_mng_inner: Arc::new(CompactManagerInner {
                store_config,
                table_config,
                engine_context,
                csr_index,
                level_controller,
                file_operator,
                compact_total_counts: AtomicU32::new(0),
                compact_trigger_counts: AtomicU32::new(0),
                compact_trigger_counts_for_drop: AtomicU32::new(0),
                compact_drop_lock: Arc::new(RwLock::new(())),
                fids_to_drop: DashMap::default(),
            }),

            join_handle_vec: Vec::with_capacity(compact_thread_num),

            stop: Arc::new(AtomicBool::new(false)),

            compact_drop_thread_pool: Arc::new(RayonThreadPool::new(compact_drop_thread_num)),
        }
    }

    pub fn start_compact_service(&mut self) {
        self.stop.store(false, Ordering::SeqCst);

        // Start work threads of multi-thread compaction.
        for _i in 0..self.cpt_mng_inner.store_config.compact_thread_num {
            let stop_tmp = Arc::clone(&self.stop);
            let inner = Arc::clone(&self.cpt_mng_inner);
            let (compact_sender, compact_receiver) = mpsc::channel();

            let handle: JoinHandle<()> = thread::spawn(move || {
                let _compaction_exit_pill = CompactionExitPill { stop: &stop_tmp };

                s_info!(
                    inner.store_config.shard_index,
                    "compact thread start to work",
                );

                let single_compactor = SingleCompactor::new(inner);
                while !stop_tmp.load(Ordering::SeqCst) {
                    {
                        let _lock_guard =
                            single_compactor.inner.level_controller.recover_read_guard();
                        let compact_status = single_compactor.run_single_compactor();
                        single_compactor
                            .inner
                            .compact_total_counts
                            .fetch_add(1, Ordering::Relaxed);

                        if compact_status == CompactStatus::CompactOk {
                            single_compactor
                                .inner
                                .compact_trigger_counts
                                .fetch_add(1, Ordering::Relaxed);

                            single_compactor
                                .inner
                                .compact_trigger_counts_for_drop
                                .fetch_add(1, Ordering::Relaxed);
                        }
                    }

                    // sleep 5 seconds by default before next round compaction.
                    if compact_receiver
                        .recv_timeout(time::Duration::from_secs(
                            single_compactor.inner.store_config.compactor_interval,
                        ))
                        .is_ok()
                    {
                        continue;
                    }
                }
            });

            self.join_handle_vec.push((handle, compact_sender));
        }

        // Output log information of multi-thread Compaction.
        let inner = Arc::clone(&self.cpt_mng_inner);
        let stop_tmp = Arc::clone(&self.stop);
        let (report_sender, report_receiver) = mpsc::channel();

        let report_handle = thread::spawn(move || {
            let _compaction_exit_pill = CompactionExitPill { stop: &stop_tmp };

            while !stop_tmp.load(Ordering::SeqCst) {
                if report_receiver
                    .recv_timeout(time::Duration::from_secs(
                        INTERVAL_SECONDS_BETWEEN_OUTPUT_COMPACTION_LOG_INFO,
                    ))
                    .is_ok()
                {
                    continue;
                }

                let compact_trigger_counts =
                    inner.compact_trigger_counts.load(Ordering::Relaxed) as f64;
                let compact_total_counts =
                    inner.compact_total_counts.load(Ordering::Relaxed) as f64;

                if compact_total_counts != 0.0 {
                    s_info!(
                        inner.store_config.shard_index,
                        "compact worker thread usage is {:.2}%",
                        compact_trigger_counts / compact_total_counts
                    );
                }

                inner.compact_trigger_counts.store(0, Ordering::SeqCst);
                inner.compact_total_counts.store(0, Ordering::SeqCst);
            }
        });
        self.join_handle_vec.push((report_handle, report_sender));

        // Drop expired index and data files in location namespace.
        let inner = Arc::clone(&self.cpt_mng_inner);
        let compact_drop_thread_pool = Arc::clone(&self.compact_drop_thread_pool);
        let stop_tmp = Arc::clone(&self.stop);
        let sleep_interval =
            inner.store_config.compactor_interval * inner.store_config.compact_drop_multiple as u64;
        let (drop_sender, drop_receiver) = mpsc::channel();

        let handle = thread::spawn(move || {
            let _compaction_exit_pill = CompactionExitPill { stop: &stop_tmp };

            while !stop_tmp.load(Ordering::SeqCst) {
                if drop_receiver
                    .recv_timeout(time::Duration::from_secs(sleep_interval))
                    .is_ok()
                {
                    continue;
                }

                CompactDropStrategy::drop_expired_index_data_files(
                    &compact_drop_thread_pool,
                    &inner,
                );
            }
        });
        self.join_handle_vec.push((handle, drop_sender));
    }

    pub fn close(&mut self) {
        self.stop.store(true, Ordering::SeqCst);

        while let Some((handle, stop_sender)) = self.join_handle_vec.pop() {
            let _ = stop_sender.send(());
            handle.join().unwrap();
        }
    }

    pub fn get_stop_state(&self) -> bool {
        self.stop.load(Ordering::SeqCst)
    }

    pub fn get_compact_drop_lock(&self) -> &RwLock<()> {
        &self.cpt_mng_inner.compact_drop_lock
    }
}

pub struct CompactManagerInner {
    // Configurations, components and operators come from cstore.
    pub store_config: Arc<StoreConfig>,
    pub table_config: Arc<TableConfig>,
    pub engine_context: Arc<EngineContext>,
    pub csr_index: Arc<CsrIndex>,
    pub level_controller: Arc<LevelController>,
    pub file_operator: Arc<CstoreFileOperator>,

    // Read & write lock which is used to make sure concurrent safety between compaction and file
    // dropping.
    pub compact_drop_lock: Arc<RwLock<()>>,

    // Report occupancy of compact thread.
    pub compact_total_counts: AtomicU32,
    pub compact_trigger_counts: AtomicU32,

    // Record the cumulative times of compactions. When reaching the configuration
    // compact_drop_multiple, the file dropping strategy is triggered async.
    pub compact_trigger_counts_for_drop: AtomicU32,

    // Record the fids of index and data files which are prepared to drop.
    pub fids_to_drop: DashMap<u32, u32>,
}
