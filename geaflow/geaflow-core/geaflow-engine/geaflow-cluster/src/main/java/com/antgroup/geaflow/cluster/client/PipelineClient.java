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

package com.antgroup.geaflow.cluster.client;

import com.antgroup.geaflow.cluster.rpc.RpcAddress;
import com.antgroup.geaflow.cluster.rpc.RpcEndpointRefFactory;
import com.antgroup.geaflow.cluster.rpc.impl.DriverEndpointRef;
import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.pipeline.IPipelineResult;
import com.antgroup.geaflow.pipeline.Pipeline;

public class PipelineClient implements IPipelineClient {

    private final DriverEndpointRef driverEndpointRef;

    public PipelineClient(RpcAddress driverAddress, Configuration config) {
        this.driverEndpointRef = RpcEndpointRefFactory.getInstance(config)
            .connectDriver(driverAddress.getHost(), driverAddress.getPort());
    }

    @Override
    public IPipelineResult submit(Pipeline pipeline) {
        return this.driverEndpointRef.executePipeline(pipeline);
    }
}
