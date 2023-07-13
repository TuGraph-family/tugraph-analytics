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

package com.antgroup.geaflow.cluster.ray.client;

import static com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys.JOB_WORK_PATH;

import com.antgroup.geaflow.cluster.client.AbstractClusterClient;
import com.antgroup.geaflow.cluster.client.IPipelineClient;
import com.antgroup.geaflow.cluster.client.PipelineClient;
import com.antgroup.geaflow.cluster.client.callback.ClusterStartedCallback.ClusterMeta;
import com.antgroup.geaflow.cluster.clustermanager.ClusterContext;
import com.antgroup.geaflow.cluster.clustermanager.ClusterInfo;
import com.antgroup.geaflow.cluster.ray.clustermanager.RayClient;
import com.antgroup.geaflow.cluster.ray.clustermanager.RayClusterId;
import com.antgroup.geaflow.cluster.ray.clustermanager.RayClusterManager;
import com.antgroup.geaflow.cluster.ray.utils.RaySystemFunc;
import com.antgroup.geaflow.common.exception.GeaflowRuntimeException;
import com.antgroup.geaflow.env.ctx.IEnvironmentContext;
import io.ray.api.Ray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RayClusterClient extends AbstractClusterClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(RayClusterClient.class);
    private RayClusterManager rayClusterManager;
    private ClusterContext clusterContext;

    @Override
    public void init(IEnvironmentContext environmentContext) {
        super.init(environmentContext);
        clusterContext = new ClusterContext(config);
        rayClusterManager = new RayClusterManager();
        rayClusterManager.init(clusterContext);
        RaySystemFunc.initRayEnv(clusterContext.getClusterConfig());
        config.put(JOB_WORK_PATH, RaySystemFunc.getWorkPath());
    }

    @Override
    public IPipelineClient startCluster() {
        try {
            RayClusterId clusterId = rayClusterManager.startMaster();
            ClusterInfo clusterInfo = RayClient.initMaster(clusterId.getHandler());
            ClusterMeta clusterMeta = new ClusterMeta(clusterInfo);
            callback.onSuccess(clusterMeta);
            LOGGER.info("cluster info: {}", clusterInfo);
            return new PipelineClient(clusterInfo.getDriverAddress(), clusterContext.getConfig());
        } catch (Throwable e) {
            LOGGER.error("deploy cluster failed", e);
            callback.onFailure(e);
            throw new GeaflowRuntimeException(e);
        }
    }

    @Override
    public void shutdown() {
        LOGGER.info("shutdown cluster");
        rayClusterManager.close();
        Ray.shutdown();
    }

}
