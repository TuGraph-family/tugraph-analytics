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

package com.antgroup.geaflow.runtime.core.scheduler.resource;

import static org.mockito.ArgumentMatchers.any;

import com.antgroup.geaflow.cluster.resourcemanager.WorkerInfo;
import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.runtime.core.scheduler.cycle.ExecutionNodeCycle;
import java.util.ArrayList;
import java.util.List;
import org.junit.Assert;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class RedoScheduledWorkerManagerTest extends BaseScheduledWorkerManagerTest {

    @Test
    public void testRequestMultiTimes() {

        RedoCycleScheduledWorkerManager wm
            = new RedoCycleScheduledWorkerManager(new Configuration());
        RedoCycleScheduledWorkerManager workerManager = Mockito.spy(wm);

        int parallelism = 3;
        ExecutionNodeCycle cycle = buildMockCycle(parallelism);
        List<WorkerInfo> workers = new ArrayList<>();
        for (int i = 0; i < parallelism; i++) {
            workers.add(new WorkerInfo("", 0, 0, 0, i, "worker-" + i));
        }
        Mockito.doReturn(workers).when(workerManager).requestWorker(any());
        workerManager.init(cycle);
        Assert.assertEquals(parallelism, workerManager.workers.size());

        for (int i = 0; i < 10; i++) {
            workerManager.assign(cycle);
            workerManager.release(cycle);
            Assert.assertEquals(parallelism, workerManager.workers.size());
        }
    }
}
