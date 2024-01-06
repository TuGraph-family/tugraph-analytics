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
import com.antgroup.geaflow.cluster.rpc.RpcEndpointRef.RpcCallback;
import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.encoder.RpcMessageEncoder;
import com.antgroup.geaflow.rpc.proto.Metrics.MetricQueryRequest;
import com.antgroup.geaflow.rpc.proto.Metrics.MetricQueryResponse;
import com.antgroup.geaflow.stats.model.MetricCache;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetricFetcher implements Serializable {
    private static final Logger LOGGER = LoggerFactory.getLogger(MetricFetcher.class);
    private static final int DEFAULT_TIMEOUT = 10000;

    private final Map<Integer, String> driverIds;
    private final MetricCache metricCache;
    private final int updateIntervalMs;
    private long lastUpdateTime;

    public MetricFetcher(Configuration configuration, IClusterManager clusterManager,
                         MetricCache metricCache) {
        this.driverIds = ((AbstractClusterManager) clusterManager).getDriverIds();
        this.metricCache = metricCache;
        this.updateIntervalMs = DEFAULT_TIMEOUT;
        RpcClient.init(configuration);
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
        Map<String, Future> futureList = new HashMap<>();
        MetricCache newMetricCache = new MetricCache();
        AtomicInteger count = new AtomicInteger(driverIds.values().size());
        for (String driverId : driverIds.values()) {
            Future<MetricQueryResponse> responseFuture = RpcClient.getInstance()
                .requestMetrics(driverId, request, new RpcCallback<MetricQueryResponse>() {
                    @Override
                    public void onSuccess(MetricQueryResponse value) {
                        MetricCache cache = RpcMessageEncoder.decode(value.getPayload());
                        newMetricCache.mergeMetricCache(cache);
                        if (count.decrementAndGet() == 0) {
                            metricCache.clearAll();
                            metricCache.mergeMetricCache(newMetricCache);
                        }
                    }

                    @Override
                    public void onFailure(Throwable t) {
                        LOGGER.warn("fail to fetch metric from " + driverId, t);
                    }
                });
            futureList.put(driverId, responseFuture);
        }
    }

}
