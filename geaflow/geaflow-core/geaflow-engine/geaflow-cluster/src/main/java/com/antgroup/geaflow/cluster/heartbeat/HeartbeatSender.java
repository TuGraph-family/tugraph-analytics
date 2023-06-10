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
import static com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys.HEARTBEAT_INTERVAL_MS;

import com.antgroup.geaflow.cluster.rpc.RpcClient;
import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.heartbeat.Heartbeat;
import com.antgroup.geaflow.common.utils.ExecutorUtil;
import com.antgroup.geaflow.common.utils.ThreadUtil;
import java.io.Serializable;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HeartbeatSender implements Serializable {
    private static final Logger LOGGER = LoggerFactory.getLogger(HeartbeatSender.class);

    private final String masterId;
    private final ScheduledExecutorService scheduledService;
    private final Supplier<Heartbeat> heartbeatTrigger;
    private final long initialDelayMs;
    private final long intervalMs;

    public HeartbeatSender(String masterId, Supplier<Heartbeat> heartbeatTrigger,
                           Configuration config) {
        this.masterId = masterId;
        this.heartbeatTrigger = heartbeatTrigger;
        this.scheduledService = new ScheduledThreadPoolExecutor(1,
            ThreadUtil.namedThreadFactory(true, "heartbeat-sender"));
        this.initialDelayMs = config.getInteger(HEARTBEAT_INITIAL_DELAY_MS);
        this.intervalMs = config.getInteger(HEARTBEAT_INTERVAL_MS);
    }

    public void start() {
        scheduledService.scheduleWithFixedDelay(() -> {
            Heartbeat message = null;
            try {
                message = heartbeatTrigger.get();
                if (message != null) {
                    RpcClient.getInstance().sendHeartBeat(masterId, message);
                }
            } catch (Throwable e) {
                LOGGER.error("send heartbeat {} failed", message, e);
            }
        }, initialDelayMs, intervalMs, TimeUnit.MILLISECONDS);
    }

    public void close() {
        if (scheduledService != null) {
            LOGGER.info("shutdown heartbeat sender thread pool");
            ExecutorUtil.shutdown(scheduledService);
        }
    }

}
