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

import com.antgroup.geaflow.cluster.rpc.ConnectAddress;
import com.antgroup.geaflow.pipeline.IPipelineResult;
import com.antgroup.geaflow.pipeline.Pipeline;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SyncPipelineClient extends AbstractPipelineClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(SyncPipelineClient.class);

    @Override
    public IPipelineResult submit(Pipeline pipeline) {
        List<IPipelineResult> results = new ArrayList<>();
        for (Map.Entry<String, ConnectAddress> entry : driverAddresses.entrySet()) {
            LOGGER.info("submit pipeline to driver {}: {}", entry.getKey(), entry.getValue());
            results.add(rpcClient.executePipeline(entry.getKey(), pipeline));
        }
        return results.get(0);
    }

    @Override
    public boolean isSync() {
        return true;
    }

    @Override
    public void close() {

    }
}
