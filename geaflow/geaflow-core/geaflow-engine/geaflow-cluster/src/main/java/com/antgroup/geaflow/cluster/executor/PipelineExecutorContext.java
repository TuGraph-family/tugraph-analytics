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

package com.antgroup.geaflow.cluster.executor;

import com.antgroup.geaflow.cluster.driver.DriverEventDispatcher;
import com.antgroup.geaflow.common.config.Configuration;
import java.util.concurrent.atomic.AtomicInteger;

public class PipelineExecutorContext {

    private DriverEventDispatcher eventDispatcher;
    private Configuration envConfig;
    private String driverId;
    private AtomicInteger idGenerator;

    public PipelineExecutorContext(String driverId, DriverEventDispatcher eventDispatcher,
                                   Configuration envConfig,
                                   AtomicInteger idGenerator) {
        this.eventDispatcher = eventDispatcher;
        this.envConfig = envConfig;
        this.driverId = driverId;
        this.idGenerator = idGenerator;
    }

    public DriverEventDispatcher getEventDispatcher() {
        return eventDispatcher;
    }

    public Configuration getEnvConfig() {
        return this.envConfig;
    }

    public String getDriverId() {
        return driverId;
    }

    public AtomicInteger getIdGenerator() {
        return idGenerator;
    }
}


