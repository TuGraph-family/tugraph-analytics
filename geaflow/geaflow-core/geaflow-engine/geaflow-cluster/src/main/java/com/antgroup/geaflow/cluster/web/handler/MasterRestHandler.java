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

import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.metric.ProcessMetrics;
import com.antgroup.geaflow.stats.collector.StatsCollectorFactory;
import java.io.Serializable;
import java.util.Map;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

@Path("/master")
public class MasterRestHandler implements Serializable {

    private final Configuration configuration;

    public MasterRestHandler(Configuration configuration) {
        this.configuration = configuration;
    }

    @GET
    @Path("/configuration")
    @Produces(MediaType.APPLICATION_JSON)
    public Map<String, String> queryConfiguration() {
        return configuration.getConfigMap();
    }

    @GET
    @Path("/metrics")
    @Produces(MediaType.APPLICATION_JSON)
    public ProcessMetrics queryProcessMetrics() {
        return StatsCollectorFactory.init(configuration).getProcessStatsCollector().collect();
    }

}
