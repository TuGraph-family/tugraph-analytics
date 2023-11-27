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

package com.antgroup.geaflow.cluster.rpc.impl;

import com.antgroup.geaflow.cluster.rpc.RpcEndpointRef;
import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.rpc.ConfigurableClientOption;
import com.baidu.brpc.client.RpcClient;
import com.baidu.brpc.client.RpcClientOptions;
import com.baidu.brpc.client.channel.Endpoint;

public abstract class AbstractRpcEndpointRef implements RpcEndpointRef {

    protected RpcClient rpcClient;
    protected final String host;
    protected final int port;
    protected final Configuration configuration;

    public AbstractRpcEndpointRef(String host, int port, Configuration configuration) {
        this.host = host;
        this.port = port;
        this.configuration = configuration;
        this.rpcClient = new RpcClient(new Endpoint(host, port), getClientOptions());
        getRpcEndpoint();
    }

    protected abstract void getRpcEndpoint();

    protected synchronized RpcClientOptions getClientOptions() {
        return ConfigurableClientOption.build(configuration);
    }

    @Override
    public void closeEndpoint() {
        close();
    }

    @Override
    public void close() {
        if (!rpcClient.isShutdown()) {
            this.rpcClient.stop();
        }
    }

}
