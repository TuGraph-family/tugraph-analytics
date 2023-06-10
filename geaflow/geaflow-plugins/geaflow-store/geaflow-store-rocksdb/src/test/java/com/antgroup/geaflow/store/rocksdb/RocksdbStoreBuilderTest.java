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

package com.antgroup.geaflow.store.rocksdb;

import static com.antgroup.geaflow.common.config.keys.FrameworkConfigKeys.JOB_MAX_PARALLEL;

import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys;
import com.antgroup.geaflow.file.FileConfigKeys;
import com.antgroup.geaflow.state.DataModel;
import com.antgroup.geaflow.state.StoreType;
import com.antgroup.geaflow.state.serializer.DefaultKVSerializer;
import com.antgroup.geaflow.store.IStoreBuilder;
import com.antgroup.geaflow.store.api.key.IKVStore;
import com.antgroup.geaflow.store.api.key.StoreBuilderFactory;
import com.antgroup.geaflow.store.context.StoreContext;
import java.io.File;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class RocksdbStoreBuilderTest {

    Map<String, String> config = new HashMap<>();

    @BeforeClass
    public void setUp() {
        FileUtils.deleteQuietly(new File("/tmp/RocksdbStoreBuilderTest"));
        config.put(ExecutionConfigKeys.JOB_APP_NAME.getKey(), "RocksdbStoreBuilderTest");
        config.put(FileConfigKeys.PERSISTENT_TYPE.getKey(), "LOCAL");
        config.put(FileConfigKeys.ROOT.getKey(), "/tmp/RocksdbStoreBuilderTest");
        config.put(JOB_MAX_PARALLEL.getKey(), "1");
    }

    @Test
    public void testKV() {
        IStoreBuilder builder = StoreBuilderFactory.build(StoreType.ROCKSDB.name());
        Configuration configuration = new Configuration(config);
        IKVStore<String, String> kvStore =
            (IKVStore<String, String>) builder.getStore(DataModel.KV, configuration);
        StoreContext storeContext = new StoreContext("rocksdb_kv").withConfig(configuration);
        storeContext.withKeySerializer(new DefaultKVSerializer<>(String.class, String.class));

        kvStore.init(storeContext);
        kvStore.put("hello", "world");
        kvStore.put("foo", "bar");
        kvStore.flush();

        Assert.assertEquals(kvStore.get("hello"), "world");
        Assert.assertEquals(kvStore.get("foo"), "bar");

        kvStore.archive(1);
        kvStore.drop();

        kvStore = (IKVStore<String, String>) builder.getStore(DataModel.KV, configuration);
        kvStore.init(storeContext);
        kvStore.recovery(1);

        Assert.assertEquals(kvStore.get("hello"), "world");
        Assert.assertEquals(kvStore.get("foo"), "bar");
    }

    @Test
    public void testFO() {
        IStoreBuilder builder = StoreBuilderFactory.build(StoreType.ROCKSDB.name());
        Configuration configuration = new Configuration(config);
        IKVStore<String, String> kvStore =
            (IKVStore<String, String>) builder.getStore(DataModel.KV, configuration);
        StoreContext storeContext = new StoreContext("rocksdb_kv").withConfig(configuration);
        storeContext.withKeySerializer(new DefaultKVSerializer<>(String.class, String.class));
        kvStore.init(storeContext);
        Assert.assertEquals(kvStore.recoveryLatest(), -1);
        for (int i = 1; i < 10; i++) {
            kvStore.put("hello", "world" + i);
            kvStore.put("foo", "bar" + i);
            kvStore.flush();
            kvStore.archive(i);
        }
        kvStore.close();
        kvStore.drop();
        kvStore = (IKVStore<String, String>) builder.getStore(DataModel.KV, configuration);
        kvStore.init(storeContext);
        kvStore.recoveryLatest();
        Assert.assertEquals(kvStore.get("hello"), "world" + 9);
        Assert.assertEquals(kvStore.get("foo"), "bar" + 9);
        kvStore.close();
        kvStore.drop();
        FileUtils.deleteQuietly(new File("/tmp/RocksdbStoreBuilderTest/RocksdbStoreBuilderTest"
            + "/rocksdb_kv/0/meta.9/_commit"));
        kvStore = (IKVStore<String, String>) builder.getStore(DataModel.KV, configuration);
        kvStore.init(storeContext);
        kvStore.recoveryLatest();
        Assert.assertEquals(kvStore.get("hello"), "world" + 8);
        Assert.assertEquals(kvStore.get("foo"), "bar" + 8);
        kvStore.close();
        kvStore.drop();
    }

    @AfterMethod
    public void tearUp() {
        FileUtils.deleteQuietly(new File("/tmp/RocksdbStoreBuilderTest"));
    }
}
