/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.antgroup.geaflow.cluster.system;

import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys;
import com.antgroup.geaflow.file.FileConfigKeys;
import com.antgroup.geaflow.state.serializer.DefaultKVSerializer;
import com.antgroup.geaflow.store.context.StoreContext;
import java.io.File;
import org.apache.commons.io.FileUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

public class RocksdbClusterMetaKVStoreTest {

    @Test
    public void testRocksdbKvStore() {
        Configuration config = new Configuration();
        FileUtils.deleteQuietly(new File("/tmp/RocksdbStoreBuilderTest"));
        config.put(ExecutionConfigKeys.JOB_APP_NAME.getKey(), "RocksdbStoreBuilderTest");
        config.put(FileConfigKeys.PERSISTENT_TYPE.getKey(), "LOCAL");
        config.put(FileConfigKeys.ROOT.getKey(), "/tmp/RocksdbStoreBuilderTest");

        IClusterMetaKVStore kvStore = new RocksdbClusterMetaKVStore<>();
        StoreContext storeContext = new StoreContext("cluster_meta_test");
        storeContext.withConfig(config);
        storeContext.withKeySerializer(new DefaultKVSerializer(null, null));
        kvStore.init(storeContext);

        kvStore.put("key1", "value1");
        kvStore.put("key2", "value2");
        kvStore.flush();
        Assert.assertEquals(kvStore.get("key1"), "value1");
        Assert.assertEquals(kvStore.get("key2"), "value2");

        kvStore.put("key1", "value1");
        kvStore.put("key3", "value3");
        kvStore.flush();
        Assert.assertEquals(kvStore.get("key1"), "value1");
        Assert.assertEquals(kvStore.get("key2"), "value2");
        Assert.assertEquals(kvStore.get("key3"), "value3");

        kvStore.remove("key1");
        kvStore.remove("key5");
        kvStore.put("key4", "value4");
        kvStore.flush();
        Assert.assertEquals(kvStore.get("key1"), null);
        Assert.assertEquals(kvStore.get("key2"), "value2");
        Assert.assertEquals(kvStore.get("key3"), "value3");
        Assert.assertEquals(kvStore.get("key4"), "value4");

        kvStore.close();
    }
}
