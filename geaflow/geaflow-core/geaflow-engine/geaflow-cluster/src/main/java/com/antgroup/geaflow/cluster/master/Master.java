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

import static com.antgroup.geaflow.cluster.constants.ClusterConstants.DEFAULT_MASTER_ID;

import com.antgroup.geaflow.cluster.clustermanager.ClusterContext;
import com.antgroup.geaflow.cluster.clustermanager.ClusterInfo;
import com.antgroup.geaflow.cluster.clustermanager.IClusterManager;
import com.antgroup.geaflow.cluster.common.AbstractComponent;
import com.antgroup.geaflow.cluster.heartbeat.HeartbeatManager;
import com.antgroup.geaflow.cluster.resourcemanager.DefaultResourceManager;
import com.antgroup.geaflow.cluster.resourcemanager.IResourceManager;
import com.antgroup.geaflow.cluster.resourcemanager.ResourceManagerContext;
import com.antgroup.geaflow.cluster.rpc.RpcAddress;
import com.antgroup.geaflow.cluster.rpc.impl.MasterEndpoint;
import com.antgroup.geaflow.cluster.rpc.impl.ResourceManagerEndpoint;
import com.antgroup.geaflow.cluster.rpc.impl.RpcServiceImpl;
import com.antgroup.geaflow.cluster.web.HttpServer;
import com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys;
import com.antgroup.geaflow.common.utils.ProcessUtil;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Master extends AbstractComponent implements IMaster {
    private static final Logger LOGGER = LoggerFactory.getLogger(Master.class);

    private IResourceManager resourceManager;
    private IClusterManager clusterManager;
    private HeartbeatManager heartbeatManager;
    private RpcAddress masterAddress;
    private HttpServer httpServer;
    private ClusterContext clusterContext;

    public Master() {
        this(0);
    }

    public Master(int rpcPort) {
        super(rpcPort);
    }

    @Override
    public void init(MasterContext context) {
        super.init(DEFAULT_MASTER_ID, context.getConfiguration().getMasterId(),
            context.getConfiguration());

        this.clusterManager = context.getClusterManager();
        this.clusterContext = context.getClusterContext();
        this.heartbeatManager = new HeartbeatManager(configuration, clusterManager);
        this.resourceManager = new DefaultResourceManager(clusterManager);
        this.clusterContext.setHeartbeatManager(heartbeatManager);
        this.clusterContext.load();
        this.clusterManager.init(clusterContext);
        startRpcService(clusterManager, resourceManager);

        // Register service info and initialize cluster.
        registerHAService();
        resourceManager.init(ResourceManagerContext.build(context, clusterContext));

        if (!configuration.getBoolean(ExecutionConfigKeys.RUN_LOCAL_MODE)) {
            httpServer = new HttpServer(configuration, clusterManager, heartbeatManager);
            httpServer.start();
        }
    }

    private void startRpcService(IClusterManager clusterManager,
                                 IResourceManager resourceManager) {
        this.rpcService = new RpcServiceImpl(rpcPort, configuration);
        this.rpcService.addEndpoint(new MasterEndpoint(this, clusterManager));
        this.rpcService.addEndpoint(new ResourceManagerEndpoint(resourceManager));
        this.rpcPort = rpcService.startService();
        this.masterAddress = new RpcAddress(ProcessUtil.getHostIp(), rpcPort);
    }

    public ClusterInfo startCluster() {
        ClusterInfo clusterInfo = new ClusterInfo();
        clusterInfo.setMasterAddress(masterAddress);
        Map<String, RpcAddress> driverAddresses = clusterManager.startDrivers();
        clusterInfo.setDriverAddresses(driverAddresses);
        LOGGER.info("init cluster with info: {}", clusterInfo);
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

