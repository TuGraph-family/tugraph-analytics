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

import com.antgroup.geaflow.cluster.resourcemanager.WorkerSnapshot;
import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys;
import com.antgroup.geaflow.common.utils.SleepUtils;
import com.antgroup.geaflow.file.FileConfigKeys;
import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import org.apache.commons.io.FileUtils;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ClusterMetaStoreTest {

    private Configuration configuration = new Configuration();

    @BeforeMethod
    public void before() {
        String path = "/tmp/" + ClusterMetaStoreTest.class.getSimpleName();
        FileUtils.deleteQuietly(new File(path));

        configuration.getConfigMap().clear();
        configuration.put(ExecutionConfigKeys.JOB_APP_NAME.getKey(), ClusterMetaStoreTest.class.getSimpleName());
        configuration.put(FileConfigKeys.PERSISTENT_TYPE.getKey(), "LOCAL");
        configuration.put(FileConfigKeys.ROOT.getKey(), path);
        configuration.put(ExecutionConfigKeys.CLUSTER_ID, "test1");

    }

    @Test
    public void testMultiThreadSave() {
        int id = 0;
        ClusterMetaStore metaStore = ClusterMetaStore.getInstance(id, "master-0", configuration);
        metaStore.getContainerIds();

        Thread thread = new Thread(() -> {
            WorkerSnapshot workerSnapshot = new WorkerSnapshot(new ArrayList<>(), new ArrayList<>());
            metaStore.saveWorkers(workerSnapshot);
            SleepUtils.sleepSecond(1);
            metaStore.flush();
        });
        thread.start();

        HashMap<Integer, String> ids = new HashMap<>();
        ids.put(1, "1");
        metaStore.saveContainerIds(ids);
        SleepUtils.sleepSecond(2);
        metaStore.flush();
    }

}