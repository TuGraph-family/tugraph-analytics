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
import com.antgroup.geaflow.cluster.rpc.RpcClient;
import com.antgroup.geaflow.common.config.Configuration;

import java.util.Map;

public abstract class AbstractPipelineClient implements IPipelineClient {

    protected RpcClient rpcClient;
    protected Map<String, ConnectAddress> driverAddresses;

    @Override
    public void init(Map<String, ConnectAddress> driverAddresses, Configuration config) {
        this.rpcClient = RpcClient.init(config);
        this.driverAddresses = driverAddresses;
    }

}
