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

package com.antgroup.geaflow.cluster.web.metrics;

import static com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys.METRIC_SERVICE_PORT;

import com.antgroup.geaflow.cluster.rpc.RpcService;
import com.antgroup.geaflow.cluster.rpc.impl.MetricEndpoint;
import com.antgroup.geaflow.cluster.rpc.impl.RpcServiceImpl;
import com.antgroup.geaflow.common.config.Configuration;
import java.io.Serializable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetricServer implements Serializable {
    private static final Logger LOGGER = LoggerFactory.getLogger(MetricServer.class);

    private final RpcService rpcService;

    public MetricServer(Configuration configuration) {
        int port = configuration.getInteger(METRIC_SERVICE_PORT);
        RpcServiceImpl rpcService = new RpcServiceImpl(port, configuration);
        rpcService.addEndpoint(new MetricEndpoint(configuration));
        this.rpcService = rpcService;
    }

    public int start() {
        int metricPort = rpcService.startService();
        LOGGER.info("started metric service on port:{}", metricPort);
        return metricPort;
    }

    public void stop() {
        LOGGER.info("stopping metric query service");
        rpcService.stopService();
    }

}
