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

package com.antgroup.geaflow.state;

import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys;
import com.antgroup.geaflow.file.FileConfigKeys;
import com.antgroup.geaflow.state.descriptor.KeyListStateDescriptor;
import com.antgroup.geaflow.state.descriptor.KeyMapStateDescriptor;
import com.antgroup.geaflow.state.descriptor.KeyValueStateDescriptor;
import com.antgroup.geaflow.utils.keygroup.DefaultKeyGroupAssigner;
import com.antgroup.geaflow.utils.keygroup.KeyGroup;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class RocksdbKeyStateTest {

    Map<String, String> config = new HashMap<>();

    @BeforeClass
    public void setUp() {
        FileUtils.deleteQuietly(new File("/tmp/geaflow/chk/"));
        FileUtils.deleteQuietly(new File("/tmp/RocksdbKeyStateTest"));
        config.put(ExecutionConfigKeys.JOB_APP_NAME.getKey(), "RocksdbKeyStateTest");
        config.put(FileConfigKeys.PERSISTENT_TYPE.getKey(), "LOCAL");
        config.put(FileConfigKeys.ROOT.getKey(), "/tmp/geaflow/chk/");
    }

    @AfterClass
    public void tearUp() {
        FileUtils.deleteQuietly(new File("/tmp/RocksdbKeyStateTest"));
    }

    @Test
    public void testKMap() {
        KeyMapStateDescriptor<String, String, String> desc =
            KeyMapStateDescriptor.build("testKV", StoreType.ROCKSDB.name());
        desc.withKeyGroup(new KeyGroup(0, 0))
            .withKeyGroupAssigner(new DefaultKeyGroupAssigner(1));
        KeyMapState<String, String, String> mapState = StateFactory.buildKeyMapState(desc,
            new Configuration(config));
        mapState.manage().operate().setCheckpointId(1L);

        Map<String, String> conf = new HashMap<>(config);
        mapState.put("hello", conf);
        mapState.add("foo", "bar1", "bar2");
        Assert.assertEquals(mapState.get("hello").size(), conf.size());
        Assert.assertEquals(mapState.get("foo").get("bar1"), "bar2");

        mapState.manage().operate().finish();
        mapState.manage().operate().archive();

        mapState.manage().operate().close();
        mapState.manage().operate().drop();

        mapState = StateFactory.buildKeyMapState(desc,
            new Configuration(config));
        mapState.manage().operate().setCheckpointId(1L);
        mapState.manage().operate().recover();

        mapState.manage().operate().setCheckpointId(2L);
        Assert.assertEquals(mapState.get("hello").size(), conf.size());
        Assert.assertEquals(mapState.get("foo").get("bar1"), "bar2");

        mapState.add("foo", "bar2", "bar3");
        mapState.manage().operate().finish();
        mapState.manage().operate().archive();

        mapState.manage().operate().close();
        mapState.manage().operate().drop();

        mapState = StateFactory.buildKeyMapState(desc,
            new Configuration(config));
        mapState.manage().operate().setCheckpointId(2L);
        mapState.manage().operate().recover();
        Assert.assertEquals(mapState.get("hello").size(), conf.size());
        Assert.assertEquals(mapState.get("foo").get("bar2"), "bar3");

        mapState.manage().operate().close();
        mapState.manage().operate().drop();
    }

    @Test
    public void testKList() {
        KeyListStateDescriptor<String, String> desc =
            KeyListStateDescriptor.build("testKList", StoreType.ROCKSDB.name());
        desc.withKeyGroup(new KeyGroup(0, 0))
            .withKeyGroupAssigner(new DefaultKeyGroupAssigner(1));
        KeyListState<String, String> listState = StateFactory.buildKeyListState(desc,
            new Configuration(config));
        listState.manage().operate().setCheckpointId(1L);

        listState.add("hello", "world");
        listState.put("foo", Arrays.asList("bar1", "bar2"));
        Assert.assertEquals(listState.get("hello"), Arrays.asList("world"));
        Assert.assertEquals(listState.get("foo"), Arrays.asList("bar1", "bar2"));

        listState.manage().operate().finish();
        listState.manage().operate().archive();

        listState.manage().operate().close();
        listState.manage().operate().drop();

        listState = StateFactory.buildKeyListState(desc, new Configuration(config));
        listState.manage().operate().setCheckpointId(1L);
        listState.manage().operate().recover();

        listState.manage().operate().setCheckpointId(2L);
        Assert.assertEquals(listState.get("hello"), Arrays.asList("world"));
        Assert.assertEquals(listState.get("foo"), Arrays.asList("bar1", "bar2"));

        listState.manage().operate().close();
        listState.manage().operate().drop();
    }

    @Test
    public void testKV() {
        KeyValueStateDescriptor<String, String> desc =
            KeyValueStateDescriptor.build("testKV", StoreType.ROCKSDB.name());
        desc.withDefaultValue(() -> "foobar").withKeyGroup(new KeyGroup(0, 0))
            .withKeyGroupAssigner(new DefaultKeyGroupAssigner(1));
        KeyValueState<String, String> valueState = StateFactory.buildKeyValueState(desc,
            new Configuration(config));

        valueState.manage().operate().setCheckpointId(1L);

        valueState.put("hello", "world");
        Assert.assertEquals(valueState.get("hello"), "world");
        Assert.assertEquals(valueState.get("foo"), "foobar");

        valueState.manage().operate().finish();
        valueState.manage().operate().archive();

        valueState.manage().operate().close();
        valueState.manage().operate().drop();

        valueState = StateFactory.buildKeyValueState(desc, new Configuration(config));
        valueState.manage().operate().setCheckpointId(1L);
        valueState.manage().operate().recover();

        valueState.manage().operate().setCheckpointId(2L);
        Assert.assertEquals(valueState.get("hello"), "world");
        Assert.assertEquals(valueState.get("foo"), "foobar");
        valueState.manage().operate().close();
        valueState.manage().operate().drop();


        desc.withTypeInfo(String.class, String.class);
        valueState = StateFactory.buildKeyValueState(desc,
            new Configuration(config));

        valueState.manage().operate().setCheckpointId(1L);

        valueState.put("hello", "world");
        Assert.assertEquals(valueState.get("hello"), "world");
        Assert.assertEquals(valueState.get("foo"), "foobar");

        valueState.manage().operate().finish();
        valueState.manage().operate().archive();

        valueState.manage().operate().close();
        valueState.manage().operate().drop();

        valueState = StateFactory.buildKeyValueState(desc, new Configuration(config));
        valueState.manage().operate().setCheckpointId(1L);
        valueState.manage().operate().recover();

        valueState.manage().operate().setCheckpointId(2L);
        Assert.assertEquals(valueState.get("hello"), "world");
        Assert.assertEquals(valueState.get("foo"), "foobar");
        valueState.manage().operate().close();
        valueState.manage().operate().drop();
    }

    @Test
    public void testKVFO() {
        KeyValueStateDescriptor<String, String> desc =
            KeyValueStateDescriptor.build("testKVFO", StoreType.ROCKSDB.name());
        desc.withKeyGroup(new KeyGroup(0, 0))
            .withKeyGroupAssigner(new DefaultKeyGroupAssigner(1));
        KeyValueState<String, String> valueState = StateFactory.buildKeyValueState(desc,
            new Configuration(config));

        for (int i = 5; i < 200; i += 5 ) {
            valueState.manage().operate().setCheckpointId(i);
            if (i > 100) {
                for (int j = 0; j < 100; j++) {
                    valueState.put("hello", "world" + j);
                }
            }
            valueState.manage().operate().finish();
            valueState.manage().operate().archive();
            if (i % 50 == 0) {
                valueState.manage().operate().close();
                valueState.manage().operate().drop();
                valueState = StateFactory.buildKeyValueState(desc,
                    new Configuration(config));
                valueState.manage().operate().setCheckpointId(i);
                valueState.manage().operate().recover();
            }
        }
    }

    @Test
    public void testScale() {
        KeyValueStateDescriptor<Integer, Integer> desc =
            KeyValueStateDescriptor.build("testScale", StoreType.ROCKSDB.name());
        desc.withKeyGroup(new KeyGroup(0, 3)).withKeyGroupAssigner(new DefaultKeyGroupAssigner(4));

        KeyValueState<Integer, Integer> valueState = StateFactory.buildKeyValueState(desc,
            new Configuration(config));

        for (int i = 0; i < 1000; i++) {
            valueState.put(i, i);
        }
        valueState.manage().operate().setCheckpointId(1);
        valueState.manage().operate().finish();
        valueState.manage().operate().archive();
        valueState.manage().operate().drop();

        desc.withKeyGroup(new KeyGroup(0, 1)).withKeyGroupAssigner(new DefaultKeyGroupAssigner(2));
        valueState = StateFactory.buildKeyValueState(desc, new Configuration(config));
        valueState.manage().operate().setCheckpointId(1);
        Exception ex = null;
        try {
            valueState.manage().operate().recover();
        } catch (Exception e) {
            ex = e;
        }
        Assert.assertNotNull(ex);
        valueState.manage().operate().drop();

        desc.withKeyGroup(new KeyGroup(0, 5)).withKeyGroupAssigner(new DefaultKeyGroupAssigner(6));
        valueState = StateFactory.buildKeyValueState(desc, new Configuration(config));
        valueState.manage().operate().setCheckpointId(1);
        ex = null;
        try {
            valueState.manage().operate().recover();
        } catch (Exception e) {
            ex = e;
        }
        Assert.assertNotNull(ex);
        valueState.manage().operate().drop();

        for (int i = 2; i < 4; i++) {
            desc.withKeyGroup(new KeyGroup(0, 7))
                .withKeyGroupAssigner(new DefaultKeyGroupAssigner(8));
            valueState = StateFactory.buildKeyValueState(desc, new Configuration(config));
            valueState.manage().operate().setCheckpointId(i - 1);
            valueState.manage().operate().recover();
            for (int j = 0; j < 1000; j++) {
                Assert.assertEquals((int)valueState.get(j), j);
            }
            valueState.manage().operate().setCheckpointId(i);
            valueState.manage().operate().archive();
            valueState.manage().operate().drop();
        }

        DefaultKeyGroupAssigner keyGroupAssigner = new DefaultKeyGroupAssigner(32);
        List<Integer> targets = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            int keyGroupId = keyGroupAssigner.assign(i);
            if (keyGroupId >= 28 && keyGroupId <= 31) {
                targets.add(i);
            }
        }

        desc.withKeyGroup(new KeyGroup(28, 31)).withKeyGroupAssigner(new DefaultKeyGroupAssigner(32));
        valueState = StateFactory.buildKeyValueState(desc, new Configuration(config));
        valueState.manage().operate().setCheckpointId(3);
        valueState.manage().operate().recover();

        for (int i: targets) {
            Assert.assertEquals(valueState.get(i), i, i);
        }
        ex = null;
        try {
            valueState.get(1);
        } catch (Exception e) {
            ex = e;
        }
        Assert.assertNotNull(ex);
        valueState.manage().operate().drop();

    }
}
