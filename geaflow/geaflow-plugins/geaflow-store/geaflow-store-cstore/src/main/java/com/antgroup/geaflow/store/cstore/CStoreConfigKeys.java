/*
 * Copyright 2023 AntGroup CO., Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */
package com.antgroup.geaflow.store.cstore;

public class CStoreConfigKeys {

    static final String CSTORE_NAME_KEY = "store.job_name";
    static final String CSTORE_LOCAL_ROOT = "local.root";
    static final String CSTORE_PERSISTENT_TYPE = "persistent.persistent_type";
    static final String CSTORE_PERSISTENT_ROOT = "persistent.root";
    static final String CSTORE_DFS_CLUSTER = "persistent.config.cluster_name";
    static final String CSTORE_OSS_BUCKET = "persistent.config.bucket";
    static final String CSTORE_OSS_ENDPOINT = "persistent.config.endpoint";
    static final String CSTORE_OSS_AK = "persistent.config.access_key_id";
    static final String CSTORE_OSS_SK = "persistent.config.access_key_secret";
}
