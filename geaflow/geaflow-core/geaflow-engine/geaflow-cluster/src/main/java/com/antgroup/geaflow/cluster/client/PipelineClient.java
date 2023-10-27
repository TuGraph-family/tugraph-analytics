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
import com.antgroup.geaflow.cluster.rpc.RpcClient;
import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.pipeline.IPipelineResult;
import com.antgroup.geaflow.pipeline.Pipeline;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class PipelineClient implements IPipelineClient {
    private static final Logger LOGGER = LoggerFactory.getLogger(PipelineClient.class);
    protected RpcClient rpcClient;
    protected Map<String, RpcAddress> driverAddresses;

    public PipelineClient(Map<String, RpcAddress> driverAddresses, Configuration config) {
        this.rpcClient = RpcClient.init(config);
        this.driverAddresses = driverAddresses;
    }

    @Override
    public IPipelineResult submit(Pipeline pipeline) {
        Map.Entry<String, RpcAddress> entry = driverAddresses.entrySet().iterator().next();
        LOGGER.info("submit pipeline to driver {}: {}", entry.getKey(), entry.getValue());
        return rpcClient.executePipeline(entry.getKey(), pipeline);
    }
}
