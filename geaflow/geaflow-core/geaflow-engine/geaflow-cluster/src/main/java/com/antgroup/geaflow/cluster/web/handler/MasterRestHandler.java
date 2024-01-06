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

import com.antgroup.geaflow.cluster.common.ComponentInfo;
import com.antgroup.geaflow.cluster.web.api.ApiResponse;
import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.metric.ProcessMetrics;
import com.antgroup.geaflow.stats.collector.StatsCollectorFactory;
import java.io.Serializable;
import java.util.Map;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("/master")
public class MasterRestHandler implements Serializable {

    private static final Logger LOGGER = LoggerFactory.getLogger(MasterRestHandler.class);

    private final Configuration configuration;
    private final ComponentInfo componentInfo;

    public MasterRestHandler(ComponentInfo componentInfo, Configuration configuration) {
        this.configuration = configuration;
        this.componentInfo = componentInfo;
    }

    @GET
    @Path("/configuration")
    @Produces(MediaType.APPLICATION_JSON)
    public ApiResponse<Map<String, String>> queryConfiguration() {
        try {
            return ApiResponse.success(configuration.getConfigMap());
        } catch (Throwable t) {
            LOGGER.error("Query master configuration failed. {}", t.getMessage(), t);
            return ApiResponse.error(t);
        }
    }

    @GET
    @Path("/metrics")
    @Produces(MediaType.APPLICATION_JSON)
    public ApiResponse<ProcessMetrics> queryProcessMetrics() {
        try {
            return ApiResponse.success(StatsCollectorFactory.init(configuration).getProcessStatsCollector().collect());
        } catch (Throwable t) {
            LOGGER.error("Query master process metrics failed. {}", t.getMessage(), t);
            return ApiResponse.error(t);
        }
    }

    @GET
    @Path("/info")
    @Produces(MediaType.APPLICATION_JSON)
    public ApiResponse<ComponentInfo> queryMasterInfo() {
        try {
            return ApiResponse.success(componentInfo);
        } catch (Throwable t) {
            LOGGER.error("Query master process metrics failed. {}", t.getMessage(), t);
            return ApiResponse.error(t);
        }
    }

}
