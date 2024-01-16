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

package com.antgroup.geaflow.metaserver.client;

import static com.antgroup.geaflow.metaserver.Constants.META_SERVER;

import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys;
import com.antgroup.geaflow.common.rpc.ConfigurableClientOption;
import com.antgroup.geaflow.common.rpc.HostAndPort;
import com.antgroup.geaflow.common.serialize.SerializerFactory;
import com.antgroup.geaflow.common.utils.RetryCommand;
import com.antgroup.geaflow.metaserver.model.protocal.MetaRequest;
import com.antgroup.geaflow.metaserver.model.protocal.request.RequestPBConverter;
import com.antgroup.geaflow.metaserver.model.protocal.response.ResponsePBConverter;
import com.antgroup.geaflow.metaserver.service.MetaServerService;
import com.antgroup.geaflow.rpc.proto.MetaServer.ServiceResultPb;
import com.antgroup.geaflow.service.discovery.ServiceBuilder;
import com.antgroup.geaflow.service.discovery.ServiceBuilderFactory;
import com.antgroup.geaflow.service.discovery.ServiceConsumer;
import com.antgroup.geaflow.service.discovery.ServiceListener;
import com.baidu.brpc.client.BrpcProxy;
import com.baidu.brpc.client.RpcClient;
import com.baidu.brpc.client.channel.Endpoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class BaseClient implements ServiceListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(BaseClient.class);
    private static final int MAX_RETRY = 5;
    private static final int RETRY_MS = 10000;
    protected ServiceConsumer serviceConsumer;
    protected RpcClient rpcClient;
    protected MetaServerService metaServerService;
    private HostAndPort currentServiceInfo;
    protected Configuration configuration;

    public BaseClient() {
    }

    public BaseClient(Configuration configuration) {
        this.configuration = configuration;
        ServiceBuilder serviceBuilder = ServiceBuilderFactory.build(
            configuration.getString(ExecutionConfigKeys.SERVICE_DISCOVERY_TYPE));
        serviceConsumer = serviceBuilder.buildConsumer(configuration);
        serviceConsumer.register(this);
        buildServerConnect(true);
    }

    protected synchronized void buildServerConnect(boolean force) {
        boolean exits = serviceConsumer.exists(META_SERVER);
        if (!exits) {
            if (!force) {
                return;
            }
            throw new IllegalStateException("not find meta server info");
        }
        byte[] bytes = serviceConsumer.getDataAndWatch(META_SERVER);
        HostAndPort serviceInfo = (HostAndPort) SerializerFactory.getKryoSerializer()
            .deserialize(bytes);
        if (currentServiceInfo != null && currentServiceInfo.equals(serviceInfo)) {
            LOGGER.info("service info {} is same, skip update", currentServiceInfo);
            return;
        }

        if (rpcClient != null) {
            rpcClient.stop();
        }

        LOGGER.info("connect to meta server {}", serviceInfo);

        rpcClient = new RpcClient(new Endpoint(serviceInfo.getHost(), serviceInfo.getPort()),
            ConfigurableClientOption.build(configuration));
        metaServerService = BrpcProxy.getProxy(rpcClient, MetaServerService.class);
        currentServiceInfo = serviceInfo;
    }

    protected <T> T process(MetaRequest request) {
        return (T) RetryCommand.run(
            () -> {
                ServiceResultPb result = metaServerService.process(RequestPBConverter.convert(request));
                return ResponsePBConverter.convert(result);
            },
            () -> {
                buildServerConnect(false);
                return true;
            },
            MAX_RETRY, RETRY_MS,true);
    }

    @Override
    public void nodeCreated(String path) {
        checkAndUpdateConnector(path);
    }

    @Override
    public void nodeDeleted(String path) {
        checkAndUpdateConnector(path);
    }

    @Override
    public void nodeDataChanged(String path) {
        checkAndUpdateConnector(path);
    }

    @Override
    public void nodeChildrenChanged(String path) {
        checkAndUpdateConnector(path);
    }

    private void checkAndUpdateConnector(String path) {
        if (path.contains(META_SERVER)) {
            buildServerConnect(false);
        }
    }

    public void close() {
        serviceConsumer.close();
        rpcClient.stop();
    }
}
