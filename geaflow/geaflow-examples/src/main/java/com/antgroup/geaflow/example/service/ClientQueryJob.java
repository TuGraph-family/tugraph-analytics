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

package com.antgroup.geaflow.example.service;

import com.antgroup.geaflow.analytics.service.client.AnalyticsClient;
import com.antgroup.geaflow.analytics.service.query.QueryResults;
import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.env.Environment;
import com.antgroup.geaflow.example.util.EnvironmentUtil;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClientQueryJob {

    private static final Logger LOGGER = LoggerFactory.getLogger(ClientQueryJob.class);

    public static void main(String[] args) {
        Environment environment = EnvironmentUtil.loadEnvironment(args);
        Configuration configuration = environment.getEnvironmentContext().getConfig();
        String graphViewName = configuration.getConfigMap()
            .get("geaflow.analytics.graph.view.name");
        Preconditions.checkNotNull(graphViewName, "graph view name is null");
        String zkNode =  configuration.getString("geaflow.zookeeper.zknode");

        String zkQuoRumServer = configuration.getString("geaflow.zookeeper.quorum.servers");
        LOGGER.info("zkQuoRumServer {}", zkQuoRumServer);
        AnalyticsClient client = AnalyticsClient.builder().withConfiguration(configuration)
            .withAnalyticsZkNode(zkNode)
            .withInitChannelPools(true)
            .withAnalyticsZkQuorumServers(zkQuoRumServer)
            .build();
        QueryResults queryResults = client.executeQuery(
            String.format("USE GRAPH %s ", graphViewName) + "; "
                + "MATCH (a) RETURN a limit 1");
        LOGGER.info("build client finish, query result {}", queryResults.getData());
    }
}