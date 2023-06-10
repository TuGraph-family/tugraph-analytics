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

package com.antgroup.geaflow.cluster.master;

import com.antgroup.geaflow.cluster.clustermanager.ClusterContext;
import com.antgroup.geaflow.cluster.clustermanager.ClusterInfo;
import com.antgroup.geaflow.cluster.clustermanager.IClusterManager;
import com.antgroup.geaflow.cluster.common.AbstractComponent;
import com.antgroup.geaflow.cluster.heartbeat.HeartbeatManager;
import com.antgroup.geaflow.cluster.resourcemanager.IResourceManager;
import com.antgroup.geaflow.cluster.resourcemanager.ResourceManagerContext;
import com.antgroup.geaflow.cluster.resourcemanager.ResourceManagerFactory;
import com.antgroup.geaflow.cluster.rpc.RpcAddress;
import com.antgroup.geaflow.cluster.rpc.impl.MasterEndpoint;
import com.antgroup.geaflow.cluster.rpc.impl.ResourceManagerEndpoint;
import com.antgroup.geaflow.cluster.rpc.impl.RpcServiceImpl;
import com.antgroup.geaflow.cluster.web.HttpServer;
import com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys;
import com.antgroup.geaflow.common.utils.ProcessUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Master extends AbstractComponent implements IMaster {
    private static final Logger LOGGER = LoggerFactory.getLogger(Master.class);

    private IResourceManager resourceManager;
    private IClusterManager clusterManager;
    private HeartbeatManager heartbeatManager;
    private RpcAddress masterAddress;
    private HttpServer httpServer;

    public Master() {
        this(0);
    }

    public Master(int rpcPort) {
        super(rpcPort);
    }

    @Override
    public void init(MasterContext context) {
        super.init(0, context.getConfiguration().getMasterId(), context.getConfiguration());
        this.clusterManager = context.getClusterManager();
        this.resourceManager = ResourceManagerFactory.build(context);

        startRpcService();
        this.masterAddress = new RpcAddress(ProcessUtil.getHostIp(), rpcPort);
        this.heartbeatManager = new HeartbeatManager(configuration, clusterManager);
        registerHAService();

        ClusterContext clusterContext = new ClusterContext(configuration);
        clusterContext.setHeartbeatManager(heartbeatManager);
        this.clusterManager.init(clusterContext);
        this.resourceManager.init(ResourceManagerContext.build(context, clusterContext));
        this.masterAddress = new RpcAddress(ProcessUtil.getHostIp(), rpcPort);

        if (!context.getConfiguration().getBoolean(ExecutionConfigKeys.RUN_LOCAL_MODE)) {
            httpServer = new HttpServer(configuration, clusterManager, heartbeatManager);
            httpServer.start();
        }
    }

    @Override
    protected void startRpcService() {
        this.rpcService = new RpcServiceImpl(rpcPort, configuration);
        this.rpcService.addEndpoint(new MasterEndpoint(this, clusterManager));
        this.rpcService.addEndpoint(new ResourceManagerEndpoint(resourceManager));
        this.rpcPort = rpcService.startService();
    }

    public ClusterInfo startCluster() {
        RpcAddress driverAddress = clusterManager.startDriver();
        ClusterInfo clusterInfo = new ClusterInfo();
        clusterInfo.setMasterAddress(masterAddress);
        clusterInfo.setDriverAddress(driverAddress);
        LOGGER.info("init with info: {}", clusterInfo);
        return clusterInfo;
    }

    @Override
    public void close() {
        super.close();
        clusterManager.close();
        if (heartbeatManager != null) {
            heartbeatManager.close();
        }
        if (httpServer != null) {
            httpServer.stop();
        }
        LOGGER.info("master {} closed", name);
    }

}

