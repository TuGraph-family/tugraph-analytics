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

package com.antgroup.geaflow.cluster.heartbeat;

import static com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys.HEARTBEAT_INITIAL_DELAY_MS;
import static com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys.HEARTBEAT_REPORT_EXPIRED_MS;
import static com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys.HEARTBEAT_REPORT_INTERVAL_MS;
import static com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys.HEARTBEAT_TIMEOUT_MS;

import com.antgroup.geaflow.cluster.clustermanager.AbstractClusterManager;
import com.antgroup.geaflow.cluster.clustermanager.IClusterManager;
import com.antgroup.geaflow.cluster.container.ContainerInfo;
import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.exception.GeaflowHeartbeatException;
import com.antgroup.geaflow.common.heartbeat.Heartbeat;
import com.antgroup.geaflow.common.heartbeat.HeartbeatInfo;
import com.antgroup.geaflow.common.heartbeat.HeartbeatInfo.ContainerHeartbeatInfo;
import com.antgroup.geaflow.common.utils.ExecutorUtil;
import com.antgroup.geaflow.common.utils.ThreadUtil;
import com.antgroup.geaflow.rpc.proto.Master.HeartbeatResponse;
import com.antgroup.geaflow.stats.collector.StatsCollectorFactory;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HeartbeatManager implements Serializable {

    private static final Logger LOGGER = LoggerFactory.getLogger(HeartbeatManager.class);

    private final long heartbeatTimeoutMs;
    private final long heartbeatReportExpiredMs;
    private final Map<Integer, Heartbeat> senderMap;
    private final IClusterManager clusterManager;

    private final ScheduledFuture<?> timeoutFuture;
    private final ScheduledFuture<?> reportFuture;
    private final ScheduledExecutorService checkTimeoutService;
    private final ScheduledExecutorService heartbeatReportService;
    private final GeaflowHeartbeatException timeoutException;

    public HeartbeatManager(Configuration config, IClusterManager clusterManager) {
        this.senderMap = new ConcurrentHashMap<>();
        this.heartbeatTimeoutMs = config.getInteger(HEARTBEAT_TIMEOUT_MS);
        int heartbeatReportMs = config.getInteger(HEARTBEAT_REPORT_INTERVAL_MS);
        int defaultReportExpiredMs = (int) ((heartbeatTimeoutMs + heartbeatReportMs) * 1.2);
        this.heartbeatReportExpiredMs = config.getInteger(HEARTBEAT_REPORT_EXPIRED_MS, defaultReportExpiredMs);

        this.checkTimeoutService = new ScheduledThreadPoolExecutor(1,
            ThreadUtil.namedThreadFactory(true, "heartbeat-check"));
        int initDelayMs = config.getInteger(HEARTBEAT_INITIAL_DELAY_MS);
        this.timeoutFuture = checkTimeoutService
            .scheduleAtFixedRate(this::checkHeartBeat, initDelayMs, heartbeatTimeoutMs,
                TimeUnit.MILLISECONDS);

        this.heartbeatReportService = new ScheduledThreadPoolExecutor(1,
            ThreadUtil.namedThreadFactory(true, "heartbeat-report"));
        this.reportFuture = heartbeatReportService
            .scheduleAtFixedRate(this::reportHeartbeat, heartbeatReportMs, heartbeatReportMs,
                TimeUnit.MILLISECONDS);

        this.clusterManager = clusterManager;
        this.timeoutException = new GeaflowHeartbeatException();
    }

    public HeartbeatResponse receivedHeartbeat(Heartbeat heartbeat) {
        senderMap.put(heartbeat.getContainerId(), heartbeat);
        boolean registered = isRegistered(heartbeat.getContainerId());
        return HeartbeatResponse.newBuilder().setSuccess(true).setRegistered(registered).build();
    }

    public void checkHeartBeat() {
        try {
            long checkTime = System.currentTimeMillis();
            AbstractClusterManager cm = (AbstractClusterManager) clusterManager;
            checkTimeout(cm.getContainerIds(), checkTime);
            checkTimeout(cm.getDriverIds(), checkTime);
        } catch (Throwable e) {
            LOGGER.warn("Catch unexpect error", e);
        }
    }

    private void checkTimeout(Map<Integer, String> map, long checkTime) {
        for (Map.Entry<Integer, String> entry : map.entrySet()) {
            int componentId = entry.getKey();
            Heartbeat heartbeat = senderMap.get(componentId);
            if (heartbeat == null) {
                if (isRegistered(componentId)) {
                    LOGGER.warn("{} is registered but heartbeat not received", entry.getValue());
                } else {
                    LOGGER.warn("{} is not registered", entry.getValue());
                }
            } else if (checkTime > heartbeat.getTimestamp() + heartbeatTimeoutMs) {
                LOGGER.error("{} heartbeat missing", entry.getValue());
                clusterManager.doFailover(componentId, timeoutException);
            }
        }
    }

    public void reportHeartbeat() {
        HeartbeatInfo heartbeatInfo = buildHeartbeatInfo();
        StatsCollectorFactory collectorFactory = StatsCollectorFactory.getInstance();
        if (collectorFactory != null) {
            collectorFactory.getHeartbeatCollector().reportHeartbeat(heartbeatInfo);
        }
    }

    protected boolean isRegistered(int componentId) {
        AbstractClusterManager cm = (AbstractClusterManager) clusterManager;
        return cm.getContainerInfos().containsKey(componentId) || cm.getDriverInfos()
            .containsKey(componentId);
    }

    protected HeartbeatInfo buildHeartbeatInfo() {
        Map<Integer, Heartbeat> heartbeatMap = getHeartBeatMap();
        Map<Integer, ContainerInfo> containerMap = ((AbstractClusterManager) clusterManager).getContainerInfos();
        Map<Integer, String> containerIndex =
            ((AbstractClusterManager) clusterManager).getContainerIds();
        int totalContainerNum = containerIndex.size();
        List<ContainerHeartbeatInfo> containerList = new ArrayList<>();
        int activeContainers = 0;
        for (Map.Entry<Integer, ContainerInfo> entry : containerMap.entrySet()) {
            ContainerHeartbeatInfo containerHeartbeatInfo = new ContainerHeartbeatInfo();
            containerHeartbeatInfo.setId(entry.getKey());
            ContainerInfo info = entry.getValue();
            containerHeartbeatInfo.setName(info.getName());
            containerHeartbeatInfo.setHost(info.getHost());
            containerHeartbeatInfo.setPid(info.getPid());
            Heartbeat heartbeat = heartbeatMap.get(entry.getKey());
            if (heartbeat != null) {
                containerHeartbeatInfo.setLastTimestamp(heartbeat.getTimestamp());
                containerHeartbeatInfo.setMetrics(heartbeat.getProcessMetrics());
                activeContainers++;
            }
            containerList.add(containerHeartbeatInfo);
        }
        HeartbeatInfo heartbeatInfo = new HeartbeatInfo();
        heartbeatInfo.setExpiredTimeMs(heartbeatReportExpiredMs);
        heartbeatInfo.setTotalNum(totalContainerNum);
        heartbeatInfo.setActiveNum(activeContainers);
        heartbeatInfo.setContainers(containerList);
        return heartbeatInfo;
    }

    public Map<Integer, Heartbeat> getHeartBeatMap() {
        return senderMap;
    }

    public Set<Integer> getActiveContainerIds() {
        Map<Integer, String> containerIdMap =
            ((AbstractClusterManager) clusterManager).getContainerIds();
        return getActiveComponentIds(containerIdMap);
    }

    public Set<Integer> getActiveDriverIds() {
        Map<Integer, String> driverIdMap = ((AbstractClusterManager) clusterManager).getDriverIds();
        return getActiveComponentIds(driverIdMap);
    }

    private Set<Integer> getActiveComponentIds(Map<Integer, String> map) {
        long checkTime = System.currentTimeMillis();
        Set<Integer> activeComponentIds = new HashSet<>();
        for (Map.Entry<Integer, String> entry : map.entrySet()) {
            int componentId = entry.getKey();
            Heartbeat heartbeat = senderMap.get(componentId);
            if (heartbeat != null && checkTime <= heartbeat.getTimestamp() + heartbeatTimeoutMs) {
                activeComponentIds.add(componentId);
            }
        }
        return activeComponentIds;
    }

    public void close() {
        if (timeoutFuture != null) {
            timeoutFuture.cancel(true);
        }
        if (checkTimeoutService != null) {
            ExecutorUtil.shutdown(checkTimeoutService);
        }
        if (reportFuture != null) {
            reportFuture.cancel(true);
        }
        if (heartbeatReportService != null) {
            ExecutorUtil.shutdown(heartbeatReportService);
        }
    }
}
