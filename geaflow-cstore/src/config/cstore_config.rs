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

// Static configuration of Geaflow CStore.
pub const CSTORE_CONFIG: &str = r#" {
    "store": {
        "location": "Local",
        "job_name": "geaflow_job",
        "store_name": "graph",
        "shard_index": "0",
        "mem_segment_size": 33554432,
        "max_buffer_size_in_segment": 4194304,
        "serialize_type": "ISerde",
        "compact_drop_thread_num": 1,
        "compact_thread_num": 4,
        "compactor_interval": 5,
        "compactor_gc_ratio": 2.0,
        "compact_drop_multiple": 10,
        "max_level": 8,
        "level0_file_num": 10,
        "level1_file_size": 268435456,
        "file_size_multiplier": 6,
        "io_thread_num": 8,
        "sort_field": "direction,disc_time,label,dst_id",
        "store_ttl": ""
    },

    "index": {
        "index_granularity": 1024,
        "index_vector_len_bit_shift": 17
    },

    "table": {
        "block_size": 65536,
        "block_capacity_critical_ratio_percentage": 85,
        "block_size_max_value": 4294967295,
        "compress_type": "Snappy"
    },

    "cache": {
        "block_cache_item_number": 1024,
        "block_cache_capacity": 268435456,
        "table_meta_cache_item_number": 1024,
        "table_meta_cache_capacity": 268435456,
        "file_cache_item_number": 1024,
        "each_file_pool_capacity": 4,
        "manifest_cache_number": 128
    },

    "dict": {
        "id_dict_key": "dict",
        "id_dict_cache_ttl_secs": 3600
    },

    "manifest": {
        "copy_buffer_size": 1024,
        "max_manifest_num": 2,
        "multi_work_threads_num": 8
    },

    "metric": {
        "reporter_type": "Prometheus",
        "enable_push_gateway": "false",
        "reporter_interval_secs": 10,
        "reporter_gw_endpoint": "http:/{push-gw-address}/metrics/job/{jobName}",
        "reporter_gw_username": "",
        "reporter_gw_password": ""
    },

    "local": {
        "file_handle_type": "Std",
        "root": "/tmp/geaflow_cstore_local"
    },

    "persistent": {
        "persistent_type": "Local",
        "root": "/tmp/geaflow_cstore_remote"
    },

    "log": {
        "enable_log": "true",
        "enable_trace": "true",
        "log_type": "ConsoleAndFile",
        "log_level": "Info"
    }
}
"#;
