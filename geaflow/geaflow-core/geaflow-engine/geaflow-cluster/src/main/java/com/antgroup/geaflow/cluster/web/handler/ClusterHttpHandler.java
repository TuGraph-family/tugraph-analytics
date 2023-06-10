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
import com.antgroup.geaflow.cluster.container.ContainerInfo;
import com.antgroup.geaflow.cluster.heartbeat.HeartbeatManager;
import com.antgroup.geaflow.common.heartbeat.Heartbeat;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class ClusterHttpHandler extends AbstractHttpHandler {

    private static final String TOTAL_CONTAINER_NUM = "totalNum";
    private static final String ACTIVE_CONTAINER_NUM = "activeNum";
    private static final String CONTAINER_LIST_KEY = "containers";
    private static final String CONTAINER_ID_KEY = "id";
    private static final String CONTAINER_NAME_KEY = "name";
    private static final String CONTAINER_HOST_KEY = "host";
    private static final String PROCESS_ID_KEY = "pid";
    private static final String PROCESS_METRICS_KEY = "metrics";
    private static final String LAST_UPDATE_TIME = "lastTimestamp";

    private final AbstractClusterManager clusterManager;
    private final HeartbeatManager heartbeatManager;

    public ClusterHttpHandler(IClusterManager clusterManager, HeartbeatManager heartbeatManager) {
        this.clusterManager = (AbstractClusterManager) clusterManager;
        this.heartbeatManager = heartbeatManager;
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        JSONObject ret = new JSONObject();
        try {
            Map<Integer, Heartbeat> heartbeatMap = heartbeatManager.getHeartBeatMap();
            Map<Integer, ContainerInfo> containerMap = clusterManager.getContainerInfos();
            Set<Integer> containerIndex = clusterManager.getContainerIds();
            int totalContainerNum = containerIndex.size();
            List<JSONObject> containerList = new ArrayList<>();
            int activeContainers = 0;
            for (Map.Entry<Integer, ContainerInfo> entry : containerMap.entrySet()) {
                JSONObject containerObj = new JSONObject();
                containerObj.put(CONTAINER_ID_KEY, entry.getKey());
                ContainerInfo info = entry.getValue();
                containerObj.put(CONTAINER_NAME_KEY, info.getName());
                containerObj.put(CONTAINER_HOST_KEY, info.getHost());
                containerObj.put(PROCESS_ID_KEY, info.getPid());
                Heartbeat heartbeat = heartbeatMap.get(entry.getKey());
                if (heartbeat != null) {
                    containerObj.put(LAST_UPDATE_TIME, heartbeat.getTimestamp());
                    containerObj.put(PROCESS_METRICS_KEY, heartbeat.getProcessMetrics());
                    activeContainers++;
                }
                containerList.add(containerObj);
            }
            ret.put(TOTAL_CONTAINER_NUM, totalContainerNum);
            ret.put(ACTIVE_CONTAINER_NUM, activeContainers);
            ret.put(CONTAINER_LIST_KEY, containerList);
            resp.setStatus(HttpServletResponse.SC_OK);
        } catch (Exception ex) {
            ret.put(ERROR_KEY, ex.getMessage());
            resp.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
        } finally {
            addHeader(resp);
            resp.getWriter().write(ret.toJSONString());
            resp.getWriter().flush();
        }
    }

}
