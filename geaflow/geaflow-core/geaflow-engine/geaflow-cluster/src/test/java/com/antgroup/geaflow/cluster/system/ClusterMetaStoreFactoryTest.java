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

package com.antgroup.geaflow.cluster.system;

import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys;
import com.antgroup.geaflow.common.config.keys.FrameworkConfigKeys;

import com.antgroup.geaflow.file.FileConfigKeys;
import java.io.File;
import org.apache.commons.io.FileUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ClusterMetaStoreFactoryTest {

    @Test
    public void testCreateMemoryKvStore() {
        Configuration configuration = new Configuration();
        configuration.put(FrameworkConfigKeys.SYSTEM_STATE_BACKEND_TYPE, "memory");
        IClusterMetaKVStore ikvStore = ClusterMetaStoreFactory.create("test", configuration);
        Assert.assertNotNull(ikvStore);
    }

    @Test
    public void testCreateRocksdbKvStore() {

        String path = "/tmp/" + ClusterMetaStoreFactoryTest.class.getSimpleName();
        FileUtils.deleteQuietly(new File(path));

        Configuration configuration = new Configuration();
        configuration.put(ExecutionConfigKeys.JOB_APP_NAME, ClusterMetaStoreFactoryTest.class.getSimpleName());
        configuration.put(FileConfigKeys.PERSISTENT_TYPE, "LOCAL");
        configuration.put(FileConfigKeys.ROOT, path);
        configuration.put(FrameworkConfigKeys.SYSTEM_STATE_BACKEND_TYPE, "rocksdb");
        configuration.put(ExecutionConfigKeys.SYSTEM_META_TABLE, "cluster_metastore_kv_test");
        IClusterMetaKVStore ikvStore = ClusterMetaStoreFactory.create("test", configuration);
        Assert.assertNotNull(ikvStore);
    }
}
