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

import com.antgroup.geaflow.cluster.rpc.IAsyncSupervisorEndpoint;
import com.antgroup.geaflow.cluster.rpc.ISupervisorEndpointRef;
import com.antgroup.geaflow.cluster.rpc.RpcEndpointRefFactory;
import com.antgroup.geaflow.cluster.rpc.RpcEndpointRefFactory.EndpointRefID;
import com.antgroup.geaflow.cluster.rpc.RpcEndpointRefFactory.EndpointType;
import com.antgroup.geaflow.cluster.rpc.RpcUtil;
import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.metaserver.client.DefaultClientOption;
import com.antgroup.geaflow.rpc.proto.Supervisor.RestartRequest;
import com.baidu.brpc.client.BrpcProxy;
import com.baidu.brpc.client.RpcClientOptions;
import com.baidu.brpc.loadbalance.LoadBalanceStrategy;
import com.google.protobuf.Empty;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

public class SupervisorEndpointRef extends AbstractRpcEndpointRef implements ISupervisorEndpointRef {

    private IAsyncSupervisorEndpoint supervisorEndpoint;
    private final EndpointRefID refID;

    public SupervisorEndpointRef(String host, int port, Configuration configuration) {
        super(host, port, configuration);
        this.refID = new EndpointRefID(host, port, EndpointType.SUPERVISOR);
    }

    @Override
    protected void getRpcEndpoint() {
        this.supervisorEndpoint = BrpcProxy.getProxy(rpcClient, IAsyncSupervisorEndpoint.class);
    }

    @Override
    protected RpcClientOptions getClientOptions() {
        RpcClientOptions options = DefaultClientOption.build();
        options.setLoadBalanceType(LoadBalanceStrategy.LOAD_BALANCE_ROUND_ROBIN);
        return options;
    }

    @Override
    public Future<Empty> restart(int pid, RpcCallback<Empty> callback) {
        CompletableFuture<Empty> result = new CompletableFuture<>();
        com.baidu.brpc.client.RpcCallback<Empty> rpcCallback =
            RpcUtil.buildRpcCallback(callback, result);
        RestartRequest request = RestartRequest.newBuilder().setPid(pid).build();
        try {
            supervisorEndpoint.restart(request, rpcCallback);
        } catch (Exception e) {
            rpcCallback.fail(e);
            RpcEndpointRefFactory.getInstance().invalidateRef(refID);
        }
        return result;
    }

}
