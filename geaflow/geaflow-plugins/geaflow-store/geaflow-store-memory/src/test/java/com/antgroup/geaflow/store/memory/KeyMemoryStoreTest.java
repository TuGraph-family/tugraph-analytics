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

package com.antgroup.geaflow.store.memory;

import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.state.DataModel;
import com.antgroup.geaflow.state.StoreType;
import com.antgroup.geaflow.store.IStoreBuilder;
import com.antgroup.geaflow.store.api.key.IKListStore;
import com.antgroup.geaflow.store.api.key.IKMapStore;
import com.antgroup.geaflow.store.api.key.IKVStore;
import com.antgroup.geaflow.store.api.key.StoreBuilderFactory;
import com.antgroup.geaflow.store.context.StoreContext;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.testng.Assert;
import org.testng.annotations.Test;

public class KeyMemoryStoreTest {

    @Test
    public void testKV() {
        IStoreBuilder builder = StoreBuilderFactory.build(StoreType.MEMORY.name());
        IKVStore<String, String> kvStore =
            (IKVStore<String, String>) builder.getStore(DataModel.KV, new Configuration());

        Configuration configuration = new Configuration();
        StoreContext storeContext = new StoreContext("mem").withConfig(configuration);
        kvStore.init(storeContext);
        kvStore.put("hello", "world");
        kvStore.put("foo", "bar");
        kvStore.flush();

        Assert.assertEquals(kvStore.get("hello"), "world");
        Assert.assertEquals(kvStore.get("foo"), "bar");

        kvStore.remove("foo");
        Assert.assertNull(kvStore.get("foo"));
    }

    @Test
    public void testKMap() {
        IStoreBuilder builder = StoreBuilderFactory.build(StoreType.MEMORY.name());
        IKMapStore<String, String, String> kMapStore =
            (IKMapStore<String, String, String>) builder.getStore(DataModel.KMap, new Configuration());

        Configuration configuration = new Configuration();
        StoreContext storeContext = new StoreContext("mem").withConfig(configuration);
        kMapStore.init(storeContext);

        Map<String, String> map = new HashMap<>();
        map.put("hello", "world");
        map.put("hello1", "world1");

        kMapStore.add("hw", map);

        map.clear();
        map.put("foo", "bar");
        kMapStore.add("hw", map);
        kMapStore.add("hw", "bar", "foo");

        Assert.assertEquals(kMapStore.get("hw").size(), 4);
        Assert.assertEquals(kMapStore.get("hw", "foo", "bar"),
            Arrays.asList("bar", "foo"));

        kMapStore.remove("hw", "bar");
        Assert.assertEquals(kMapStore.get("hw").size(), 3);

        kMapStore.remove("hw");
        Assert.assertEquals(kMapStore.get("hw").size(), 0);
    }

    @Test
    public void testKList() {
        IStoreBuilder builder = StoreBuilderFactory.build(StoreType.MEMORY.name());
        IKListStore<String, String> kListStore =
            (IKListStore<String, String>) builder.getStore(DataModel.KList, new Configuration());

        Configuration configuration = new Configuration();
        StoreContext storeContext = new StoreContext("mem").withConfig(configuration);
        kListStore.init(storeContext);

        kListStore.add("hw", "foo", "bar");
        kListStore.add("hw", "hello");

        Assert.assertEquals(kListStore.get("hw").size(), 3);
        kListStore.remove("hw");
        Assert.assertEquals(kListStore.get("hw").size(), 0);
    }
}
