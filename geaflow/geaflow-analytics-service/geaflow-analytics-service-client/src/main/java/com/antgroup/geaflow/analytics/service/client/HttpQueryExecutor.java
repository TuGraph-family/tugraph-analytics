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

package com.antgroup.geaflow.analytics.service.client;

import com.antgroup.geaflow.analytics.service.client.jdbc.HttpQueryChannel;
import com.antgroup.geaflow.common.exception.GeaflowRuntimeException;
import com.antgroup.geaflow.common.rpc.HostAndPort;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HttpQueryExecutor extends AbstractQueryExecutor {

    private static final Logger LOGGER = LoggerFactory.getLogger(HttpQueryExecutor.class);

    private final Map<HostAndPort, HttpQueryChannel> coordinatorAddress2Stub;

    public HttpQueryExecutor(ClientHandlerContext context) {
        super(context);
        this.coordinatorAddress2Stub = new HashMap<>();
    }

    @Override
    protected void initManagedChannel(HostAndPort address) {
        HttpQueryChannel httpQueryChannel = this.coordinatorAddress2Stub.get(address);
        if (httpQueryChannel == null) {
            this.coordinatorAddress2Stub.put(address, new HttpQueryChannel(config, timeoutMs));
        }
    }

    @Override
    protected void shutdown() {
        for (Entry<HostAndPort, HttpQueryChannel> entry : this.coordinatorAddress2Stub.entrySet()) {
            HostAndPort address = entry.getKey();
            HttpQueryChannel channel = entry.getValue();
            if (channel != null) {
                try {
                    channel.close();
                } catch (Exception e) {
                    LOGGER.warn("coordinator [{}:{}] shutdown failed", address.getHost(),
                        address.getPort(), e);
                    throw new GeaflowRuntimeException(String.format("coordinator [%s:%d] "
                        + "shutdown error", address.getHost(), address.getPort()), e);
                }
            }
        }
        LOGGER.info("shutdown all channel");
    }


    public HttpQueryChannel getHttpQueryChannel(HostAndPort address) {
        initManagedChannel(address);
        HttpQueryChannel httpQueryChannel = this.coordinatorAddress2Stub.get(address);
        return httpQueryChannel;
    }

}
