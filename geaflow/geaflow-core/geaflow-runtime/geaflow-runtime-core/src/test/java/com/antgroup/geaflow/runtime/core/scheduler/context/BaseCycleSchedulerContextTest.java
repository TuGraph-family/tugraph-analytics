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

package com.antgroup.geaflow.runtime.core.scheduler.context;

import static org.mockito.ArgumentMatchers.any;

import com.antgroup.geaflow.cluster.resourcemanager.WorkerInfo;
import com.antgroup.geaflow.cluster.system.ClusterMetaStore;
import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys;
import com.antgroup.geaflow.file.FileConfigKeys;
import com.antgroup.geaflow.runtime.core.scheduler.cycle.ExecutionNodeCycle;
import com.antgroup.geaflow.runtime.core.scheduler.cycle.IExecutionCycle;
import com.antgroup.geaflow.runtime.core.scheduler.resource.IScheduledWorkerManager;
import com.antgroup.geaflow.runtime.core.scheduler.resource.ScheduledWorkerManagerFactory;
import java.io.File;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;

public class BaseCycleSchedulerContextTest {

    protected static MockedStatic<ScheduledWorkerManagerFactory> mockWorkerManager;
    protected Configuration configuration = new Configuration();

    @BeforeMethod
    public void setUp() {
        ClusterMetaStore.close();
        mockWorkerManager = mockScheduledWorkerManager();

        String path = "/tmp/" + this.getClass().getSimpleName();
        FileUtils.deleteQuietly(new File(path));
        configuration.getConfigMap().clear();
        configuration.put(ExecutionConfigKeys.JOB_APP_NAME.getKey(), this.getClass().getSimpleName());
        configuration.put(FileConfigKeys.PERSISTENT_TYPE.getKey(), "LOCAL");
        configuration.put(FileConfigKeys.ROOT.getKey(), path);
        configuration.put(ExecutionConfigKeys.JOB_UNIQUE_ID, "test-scheduler-context");
    }

    @AfterMethod
    public void cleanUp() {
        mockWorkerManager.close();
        String path = "/tmp/" + this.getClass().getSimpleName();
        FileUtils.deleteQuietly(new File(path));
        ClusterMetaStore.close();
    }

    public static MockedStatic<ScheduledWorkerManagerFactory> mockScheduledWorkerManager() {
        MockedStatic<ScheduledWorkerManagerFactory> ms = Mockito.mockStatic(ScheduledWorkerManagerFactory.class);
        ms.when(() ->
                ScheduledWorkerManagerFactory.createScheduledWorkerManager(any(), any()))
            .then(invocation -> new MockScheduledWorkerManager());
        return ms;
    }

    protected static class MockScheduledWorkerManager
        implements IScheduledWorkerManager<IExecutionCycle, ExecutionNodeCycle> {

        @Override
        public void init(IExecutionCycle graph) {
        }

        @Override
        public List<WorkerInfo> assign(ExecutionNodeCycle vertex) {
            return null;
        }

        @Override
        public void release(ExecutionNodeCycle vertex) {
        }

        @Override
        public void clean(CleanWorkerFunction cleaFunc) {
        }

        @Override
        public void close() {
        }
    }
}
