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

package com.antgroup.geaflow.cluster.web.handler;

import com.alibaba.fastjson.JSONObject;
import com.antgroup.geaflow.cluster.clustermanager.AbstractClusterManager;
import com.antgroup.geaflow.cluster.clustermanager.IClusterManager;
import com.antgroup.geaflow.cluster.common.ComponentInfo;
import com.antgroup.geaflow.cluster.container.ContainerInfo;
import com.antgroup.geaflow.cluster.driver.DriverInfo;
import com.antgroup.geaflow.cluster.heartbeat.HeartbeatManager;
import com.antgroup.geaflow.cluster.resourcemanager.DefaultResourceManager;
import com.antgroup.geaflow.cluster.resourcemanager.IResourceManager;
import com.antgroup.geaflow.cluster.web.metrics.MetricFetcher;
import com.antgroup.geaflow.cluster.web.metrics.ResourceMetrics;
import com.antgroup.geaflow.common.heartbeat.Heartbeat;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

@Path("/")
public class ClusterRestHandler implements Serializable {

    private static final String TOTAL_CONTAINER_NUM = "totalContainers";
    private static final String ACTIVE_CONTAINER_NUM = "activeContainers";
    private static final String ALLOCATED_WORKER_NUM = "allocatedWorkers";
    private static final String AVAILABLE_WORKER_NUM = "availableWorkers";
    private static final String PENDING_WORKER_NUM = "pendingWorkers";

    private static final String CONTAINER_ID_KEY = "id";
    private static final String CONTAINER_NAME_KEY = "name";
    private static final String CONTAINER_HOST_KEY = "host";
    private static final String PROCESS_ID_KEY = "pid";
    private static final String PROCESS_METRICS_KEY = "metrics";
    private static final String LAST_UPDATE_TIME = "lastTimestamp";

    private final AbstractClusterManager clusterManager;
    private final HeartbeatManager heartbeatManager;
    private final IResourceManager resourceManager;
    private final MetricFetcher metricFetcher;

    public ClusterRestHandler(IClusterManager clusterManager, HeartbeatManager heartbeatManager,
                              IResourceManager resourceManager, MetricFetcher metricFetcher) {
        this.clusterManager = (AbstractClusterManager) clusterManager;
        this.heartbeatManager = heartbeatManager;
        this.resourceManager = resourceManager;
        this.metricFetcher = metricFetcher;
    }

    @GET
    @Path("/overview")
    @Produces(MediaType.APPLICATION_JSON)
    public Map<String, Object> getOverview() throws IOException {
        metricFetcher.update();
        int totalContainerNum = clusterManager.getTotalContainers();
        int activeContainers = heartbeatManager.getHeartBeatMap().size();
        Map<String, Object> ret = new HashMap<>();
        ret.put(TOTAL_CONTAINER_NUM, totalContainerNum);
        ret.put(ACTIVE_CONTAINER_NUM, activeContainers);
        if (resourceManager instanceof DefaultResourceManager) {
            ResourceMetrics metrics = ((DefaultResourceManager) resourceManager)
                .getResourceMetrics();
            ret.put(ALLOCATED_WORKER_NUM, metrics.getTotalWorkers());
            ret.put(AVAILABLE_WORKER_NUM, metrics.getAvailableWorkers());
            ret.put(PENDING_WORKER_NUM, metrics.getPendingWorkers());
        }
        return ret;
    }

    @GET
    @Path("/containers")
    @Produces(MediaType.APPLICATION_JSON)
    public List<JSONObject> getContainers() throws IOException {
        metricFetcher.update();
        Map<Integer, ContainerInfo> containerMap = clusterManager.getContainerInfos();
        Map<Integer, Heartbeat> heartbeatMap = heartbeatManager.getHeartBeatMap();
        return buildDetailInfo(containerMap, heartbeatMap);
    }

    @GET
    @Path("/drivers")
    @Produces(MediaType.APPLICATION_JSON)
    public List<JSONObject> getDrivers() throws IOException {
        metricFetcher.update();
        Map<Integer, DriverInfo> driverMap = clusterManager.getDriverInfos();
        Map<Integer, Heartbeat> heartbeatMap = heartbeatManager.getHeartBeatMap();
        return buildDetailInfo(driverMap, heartbeatMap);
    }

    private <T extends ComponentInfo> List<JSONObject> buildDetailInfo(
        Map<Integer, T> componentMap, Map<Integer, Heartbeat> heartbeatMap) {
        List<JSONObject> result = new ArrayList<>();
        for (Map.Entry<Integer, T> entry : componentMap.entrySet()) {
            JSONObject containerObj = new JSONObject();
            containerObj.put(CONTAINER_ID_KEY, entry.getKey());
            ComponentInfo info = entry.getValue();
            containerObj.put(CONTAINER_NAME_KEY, info.getName());
            containerObj.put(CONTAINER_HOST_KEY, info.getHost());
            containerObj.put(PROCESS_ID_KEY, info.getPid());
            Heartbeat heartbeat = heartbeatMap.get(entry.getKey());
            if (heartbeat != null) {
                containerObj.put(LAST_UPDATE_TIME, heartbeat.getTimestamp());
                containerObj.put(PROCESS_METRICS_KEY, heartbeat.getProcessMetrics());
            }
            result.add(containerObj);
        }
        return result;
    }
}
