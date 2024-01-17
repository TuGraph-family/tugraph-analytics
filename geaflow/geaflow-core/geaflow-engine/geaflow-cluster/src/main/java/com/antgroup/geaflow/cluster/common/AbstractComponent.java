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

package com.antgroup.geaflow.cluster.common;

import com.antgroup.geaflow.cluster.rpc.RpcClient;
import com.antgroup.geaflow.cluster.rpc.impl.RpcServiceImpl;
import com.antgroup.geaflow.cluster.system.ClusterMetaStore;
import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.utils.ProcessUtil;
import com.antgroup.geaflow.ha.service.HAServiceFactory;
import com.antgroup.geaflow.ha.service.IHAService;
import com.antgroup.geaflow.ha.service.ResourceData;
import com.antgroup.geaflow.metrics.common.MetricGroupRegistry;
import com.antgroup.geaflow.metrics.common.api.MetricGroup;
import com.antgroup.geaflow.shuffle.service.ShuffleManager;
import com.antgroup.geaflow.stats.collector.StatsCollectorFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractComponent {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractComponent.class);

    protected int id;
    protected String name;
    protected String masterId;
    protected int rpcPort;
    protected int supervisorPort;

    protected Configuration configuration;
    protected IHAService haService;
    protected RpcServiceImpl rpcService;
    protected MetricGroup metricGroup;
    protected MetricGroupRegistry metricGroupRegistry;

    public AbstractComponent() {
    }

    public AbstractComponent(int rpcPort) {
        this.rpcPort = rpcPort;
    }

    public void init(int id, String name, Configuration configuration) {
        this.id = id;
        this.name = name;
        this.configuration = configuration;
        this.masterId = configuration.getMasterId();

        this.metricGroupRegistry = MetricGroupRegistry.getInstance(configuration);
        this.metricGroup = metricGroupRegistry.getMetricGroup();
        this.haService = HAServiceFactory.getService(configuration);

        RpcClient.init(configuration);
        ClusterMetaStore.init(id, name, configuration);
        StatsCollectorFactory.init(configuration);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            // Use stderr here since the logger may have been reset by its JVM shutdown hook.
            LOGGER.warn("*** Shutting ClusterMetaStore since JVM is shutting down.");
            ClusterMetaStore.close();
            LOGGER.warn("*** ClusterMetaStore is shutdown.");
        }));
    }

    protected void registerHAService() {
        ResourceData resourceData = buildResourceData();
        LOGGER.info("register {}: {}", name, resourceData);
        haService.register(name, resourceData);
    }
    
    protected ResourceData buildResourceData() {
        ResourceData resourceData = new ResourceData();
        resourceData.setProcessId(ProcessUtil.getProcessId());
        resourceData.setHost(ProcessUtil.getHostIp());
        resourceData.setRpcPort(rpcPort);
        ShuffleManager shuffleManager = ShuffleManager.getInstance();
        if (shuffleManager != null) {
            resourceData.setShufflePort(shuffleManager.getShufflePort());
        }
        return resourceData;
    }

    public void close() {
        if (haService != null) {
            haService.close();
        }
        if (rpcService != null) {
            rpcService.stopService();
        }
        ClusterMetaStore.close();
    }

    public void waitTermination() {
        rpcService.waitTermination();
    }

}
