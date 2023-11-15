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

import com.antgroup.geaflow.cluster.rpc.IAsyncMetricEndpoint;
import com.antgroup.geaflow.cluster.rpc.IMetricEndpointRef;
import com.antgroup.geaflow.cluster.rpc.RpcUtil;
import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.metaserver.client.DefaultClientOption;
import com.antgroup.geaflow.rpc.proto.Metrics.MetricQueryRequest;
import com.antgroup.geaflow.rpc.proto.Metrics.MetricQueryResponse;
import com.baidu.brpc.client.BrpcProxy;
import com.baidu.brpc.client.RpcClientOptions;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

public class MetricEndpointRef extends AbstractRpcEndpointRef implements IMetricEndpointRef {

    private IAsyncMetricEndpoint metricEndpoint;

    public MetricEndpointRef(String host, int port, Configuration configuration) {
        super(host, port, configuration);
    }

    @Override
    protected void getRpcEndpoint() {
        this.metricEndpoint = BrpcProxy.getProxy(rpcClient, IAsyncMetricEndpoint.class);
    }

    @Override
    protected RpcClientOptions getClientOptions() {
        return DefaultClientOption.build();
    }

    @Override
    public Future<MetricQueryResponse> queryMetrics(MetricQueryRequest request, RpcCallback<MetricQueryResponse> callback) {
        CompletableFuture<MetricQueryResponse> result = new CompletableFuture<>();
        com.baidu.brpc.client.RpcCallback<MetricQueryResponse> rpcCallback =
            RpcUtil.buildRpcCallback(
            callback, result);
        this.metricEndpoint.queryMetrics(request, rpcCallback);
        return result;
    }

}
