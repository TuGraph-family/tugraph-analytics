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

package com.antgroup.geaflow.cluster.driver;

import static org.mockito.Matchers.any;

import com.antgroup.geaflow.cluster.system.ClusterMetaStore;
import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys;
import com.antgroup.geaflow.env.Environment;
import com.antgroup.geaflow.file.FileConfigKeys;
import com.antgroup.geaflow.pipeline.Pipeline;
import com.antgroup.geaflow.pipeline.PipelineFactory;
import java.io.File;
import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.mockito.Mockito;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class DriverContextTest {

    private Configuration configuration = new Configuration();

    @BeforeMethod
    public void before() {
        String path = "/tmp/" + DriverContextTest.class.getSimpleName();
        FileUtils.deleteQuietly(new File(path));

        configuration.getConfigMap().clear();
        configuration.put(ExecutionConfigKeys.JOB_APP_NAME.getKey(), DriverContextTest.class.getSimpleName());
        configuration.put(FileConfigKeys.PERSISTENT_TYPE.getKey(), "LOCAL");
        configuration.put(FileConfigKeys.ROOT.getKey(), path);
        configuration.put(ExecutionConfigKeys.CLUSTER_ID, "test1");
    }

    @AfterMethod
    public void after() {
        String path = "/tmp/" + DriverContextTest.class.getSimpleName();
        FileUtils.deleteQuietly(new File(path));
        ClusterMetaStore.close();
    }

    @Test
    public void testRecoverContext() {

        int driverId = 1;
        ClusterMetaStore.init(driverId, configuration);
        DriverContext driverContext = new DriverContext(driverId, configuration);

        Environment environment = Mockito.mock(Environment.class);
        Mockito.doNothing().when(environment).addPipeline(any());
        Pipeline pipeline = PipelineFactory.buildPipeline(environment);
        driverContext.addPipeline(pipeline);
        driverContext.addFinishedPipelineTask(0);
        driverContext.addFinishedPipelineTask(1);
        driverContext.checkpoint(new DriverContext.PipelineCheckpointFunction());
        driverContext.checkpoint(new DriverContext.PipelineTaskCheckpointFunction());

        DriverContext newContext = new DriverContext(driverId, configuration);
        newContext.load();

        Assert.assertNotNull(pipeline);
        Assert.assertEquals(2, newContext.getFinishedPipelineTasks().size());
        Assert.assertEquals(0, newContext.getFinishedPipelineTasks().get(0).intValue());
        Assert.assertEquals(1, newContext.getFinishedPipelineTasks().get(1).intValue());

        // ---- mock restart job ----
        // cluster id is changed, re-init cluster metastore.
        ClusterMetaStore.close();
        configuration.put(ExecutionConfigKeys.CLUSTER_ID, "test2");
        ClusterMetaStore.init(driverId, configuration);
        // rebuild, context reliable event list is empty, and metastore is cleaned.
        DriverContext restarted = new DriverContext(driverId, configuration);
        restarted.load();
        Assert.assertNull(restarted.getPipeline());
        Assert.assertTrue(restarted.getFinishedPipelineTasks().isEmpty());
    }
}
