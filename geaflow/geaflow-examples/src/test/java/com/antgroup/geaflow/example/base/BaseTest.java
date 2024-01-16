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

package com.antgroup.geaflow.example.base;

import com.antgroup.geaflow.cluster.system.ClusterMetaStore;
import com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys;
import com.antgroup.geaflow.env.Environment;
import com.antgroup.geaflow.example.config.ExampleConfigKeys;
import com.antgroup.geaflow.example.util.ExampleSinkFunctionFactory.SinkType;
import com.antgroup.geaflow.runtime.core.scheduler.resource.AbstractScheduledWorkerManager;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;

public class BaseTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(BaseTest.class);

    protected Map<String, String> config;
    protected Environment environment;

    @BeforeMethod
    public void cleanMetaStore() {
        LOGGER.info("clean cluster meta store");
        ClusterMetaStore.close();
        environment = null;
    }

    @BeforeMethod
    public void setup() {
        config = new HashMap<>();
        config.put(ExampleConfigKeys.GEAFLOW_SINK_TYPE.getKey(), SinkType.FILE_SINK.name());
        config.put(ExecutionConfigKeys.HTTP_REST_SERVICE_ENABLE.getKey(), "false");
    }

    @AfterMethod
    public void clean() {
        if (environment != null) {
            environment.shutdown();
        }
        ClusterMetaStore.close();
        AbstractScheduledWorkerManager.closeInstance();
    }

    public Map<String, String> getConfig() {
        return config;
    }
}
