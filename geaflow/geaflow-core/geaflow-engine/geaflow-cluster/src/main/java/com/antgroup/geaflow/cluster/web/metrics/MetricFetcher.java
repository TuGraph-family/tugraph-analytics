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

package com.antgroup.geaflow.cluster.web.metrics;

import com.antgroup.geaflow.cluster.clustermanager.AbstractClusterManager;
import com.antgroup.geaflow.cluster.clustermanager.IClusterManager;
import com.antgroup.geaflow.cluster.rpc.RpcClient;
import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.encoder.RpcMessageEncoder;
import com.antgroup.geaflow.rpc.proto.Metrics.MetricQueryRequest;
import com.antgroup.geaflow.rpc.proto.Metrics.MetricQueryResponse;
import com.antgroup.geaflow.stats.model.MetricCache;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetricFetcher implements Serializable {
    private static final Logger LOGGER = LoggerFactory.getLogger(MetricFetcher.class);
    private static final int DEFAULT_TIMEOUT = 10000;

    private final Map<Integer, String> driverIds;
    private final ExecutorService executorService;
    private final MetricCache metricCache;
    private final int updateIntervalMs;
    private long lastUpdateTime;

    public MetricFetcher(Configuration configuration, IClusterManager clusterManager,
                         MetricCache metricCache) {
        this.driverIds = ((AbstractClusterManager) clusterManager).getDriverIds();
        this.metricCache = metricCache;
        this.updateIntervalMs = DEFAULT_TIMEOUT;
        RpcClient.init(configuration);
        executorService = RpcClient.getInstance().getExecutor();
    }

    public synchronized void update() {
        long currentTime = System.currentTimeMillis();
        if (lastUpdateTime + updateIntervalMs <= currentTime) {
            lastUpdateTime = currentTime;
            fetch();
        }
    }

    private void fetch() {
        MetricQueryRequest request = MetricQueryRequest.newBuilder().build();
        Map<String, ListenableFuture> futureList = new HashMap<>();
        for (String driverId : driverIds.values()) {
            ListenableFuture<MetricQueryResponse> responseFuture = RpcClient.getInstance()
                .requestMetrics(driverId, request);
            futureList.put(driverId, responseFuture);
        }
        MetricCache newMetricCache = new MetricCache();
        AtomicInteger count = new AtomicInteger(futureList.size());
        for (Entry<String, ListenableFuture> entry : futureList.entrySet()) {
            Futures.addCallback(entry.getValue(), new FutureCallback<MetricQueryResponse>() {

                @Override
                public void onSuccess(@Nullable MetricQueryResponse result) {
                    MetricCache cache = RpcMessageEncoder.decode(result.getPayload());
                    newMetricCache.mergeMetricCache(cache);
                    if (count.decrementAndGet() == 0) {
                        metricCache.clearAll();
                        metricCache.mergeMetricCache(newMetricCache);
                    }
                }

                @Override
                public void onFailure(Throwable t) {
                    LOGGER.warn("fail to fetch metric from " + entry.getKey(), t);
                }
            }, executorService);
        }
    }

}
