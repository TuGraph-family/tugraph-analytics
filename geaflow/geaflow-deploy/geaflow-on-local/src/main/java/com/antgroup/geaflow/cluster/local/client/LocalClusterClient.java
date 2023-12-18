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

package com.antgroup.geaflow.cluster.local.client;

import com.antgroup.geaflow.cluster.client.AbstractClusterClient;
import com.antgroup.geaflow.cluster.client.IPipelineClient;
import com.antgroup.geaflow.cluster.client.PipelineClientFactory;
import com.antgroup.geaflow.cluster.client.callback.ClusterStartedCallback.ClusterMeta;
import com.antgroup.geaflow.cluster.clustermanager.ClusterContext;
import com.antgroup.geaflow.cluster.clustermanager.ClusterInfo;
import com.antgroup.geaflow.cluster.local.clustermanager.LocalClient;
import com.antgroup.geaflow.cluster.local.clustermanager.LocalClusterId;
import com.antgroup.geaflow.cluster.local.clustermanager.LocalClusterManager;
import com.antgroup.geaflow.common.exception.GeaflowRuntimeException;
import com.antgroup.geaflow.common.utils.ThreadUtil;
import com.antgroup.geaflow.env.ctx.IEnvironmentContext;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LocalClusterClient extends AbstractClusterClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(LocalClusterClient.class);
    private LocalClusterManager localClusterManager;
    private ClusterContext clusterContext;
    private final ExecutorService agentService = Executors.newSingleThreadExecutor(
        ThreadUtil.namedThreadFactory(true, "local-agent"));

    @Override
    public void init(IEnvironmentContext environmentContext) {
        super.init(environmentContext);
        clusterContext = new ClusterContext(config);
        localClusterManager = new LocalClusterManager();
        localClusterManager.init(clusterContext);
    }

    @Override
    public IPipelineClient startCluster() {
        try {
            LocalClusterId clusterId = localClusterManager.startMaster();
            ClusterInfo clusterInfo = LocalClient.initMaster(clusterId.getMaster());
            ClusterMeta clusterMeta = new ClusterMeta(clusterInfo);
            callback.onSuccess(clusterMeta);
            LOGGER.info("cluster info: {}", clusterInfo);
            return PipelineClientFactory.createPipelineClient(
                clusterInfo.getDriverAddresses(), clusterContext.getConfig());
        } catch (Throwable e) {
            LOGGER.error("deploy cluster failed", e);
            callback.onFailure(e);
            throw new GeaflowRuntimeException(e);
        }
    }

    @Override
    public void shutdown() {
        LOGGER.info("shutdown cluster");
        localClusterManager.close();
    }

}
