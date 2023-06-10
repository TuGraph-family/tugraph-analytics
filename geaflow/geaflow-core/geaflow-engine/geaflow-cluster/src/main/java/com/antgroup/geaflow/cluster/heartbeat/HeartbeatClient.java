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

import com.antgroup.geaflow.cluster.rpc.RpcClient;
import com.antgroup.geaflow.cluster.rpc.RpcEndpointRef.RpcCallback;
import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.heartbeat.Heartbeat;
import com.antgroup.geaflow.rpc.proto.Master.RegisterResponse;
import com.antgroup.geaflow.stats.collector.ProcessStatsCollector;
import com.antgroup.geaflow.stats.collector.StatsCollectorFactory;
import java.io.Serializable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HeartbeatClient implements Serializable {
    private static final Logger LOGGER = LoggerFactory.getLogger(HeartbeatClient.class);

    private final int containerId;
    private final String containerName;
    private final Configuration config;
    private HeartbeatSender heartbeatSender;
    private final ProcessStatsCollector statsCollector;
    private String masterId;

    public HeartbeatClient(int containerId, String containerName, Configuration config) {
        this.containerId = containerId;
        this.containerName = containerName;
        this.config = config;
        this.statsCollector = StatsCollectorFactory.getInstance().getProcessStatsCollector();
    }

    public <T> void registerToMaster(String masterId, T info) {
        this.masterId = masterId;
        LOGGER.info("register {} info:{}", containerName, info);
        RpcClient.init(config);
        doRegister(masterId, info);
    }

    private <T> void doRegister(String masterId, T info) {
        RpcClient.getInstance().registerContainer(masterId, info, new RpcCallback<RegisterResponse>() {

            @Override
            public void onSuccess(RegisterResponse event) {
                LOGGER.info("{} registered success:{}", containerName, event.getSuccess());
                startHeartBeat(masterId);
            }

            @Override
            public void onFailure(Throwable t) {
                LOGGER.error("register info failed", t);
            }
        });
    }

    public void startHeartBeat(String masterId) {
        LOGGER.info("start {} heartbeat", containerName);
        this.heartbeatSender = new HeartbeatSender(masterId, () -> {
            Heartbeat heartbeat = null;
            if (containerName != null) {
                heartbeat = new Heartbeat(containerId);
                heartbeat.setProcessMetrics(statsCollector.collect());
            }
            return heartbeat;
        }, config);

        this.heartbeatSender.start();
    }

    public void close() {
        if (heartbeatSender != null) {
            heartbeatSender.close();
        }
    }

}
