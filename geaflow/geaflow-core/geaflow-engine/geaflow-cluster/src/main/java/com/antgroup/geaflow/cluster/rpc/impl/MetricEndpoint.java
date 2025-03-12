/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.antgroup.geaflow.cluster.rpc.impl;

import com.antgroup.geaflow.cluster.rpc.IMetricEndpoint;
import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.encoder.RpcMessageEncoder;
import com.antgroup.geaflow.common.exception.GeaflowRuntimeException;
import com.antgroup.geaflow.rpc.proto.Metrics.MetricQueryRequest;
import com.antgroup.geaflow.rpc.proto.Metrics.MetricQueryResponse;
import com.antgroup.geaflow.stats.collector.StatsCollectorFactory;
import com.antgroup.geaflow.stats.model.MetricCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetricEndpoint implements IMetricEndpoint {

    private static final Logger LOGGER = LoggerFactory.getLogger(MetricEndpoint.class);
    private final Configuration configuration;

    public MetricEndpoint(Configuration configuration) {
        this.configuration = configuration;
    }

    @Override
    public MetricQueryResponse queryMetrics(MetricQueryRequest request) {
        try {
            MetricCache cache = StatsCollectorFactory.init(configuration).getMetricCache();
            MetricQueryResponse.Builder builder = MetricQueryResponse.newBuilder();
            builder.setPayload(RpcMessageEncoder.encode(cache));
            return builder.build();
        } catch (Throwable t) {
            LOGGER.error("process request failed: {}", t.getMessage(), t);
            throw new GeaflowRuntimeException(String.format("process request failed: %s", t.getMessage()),
                t);
        }
    }
}
