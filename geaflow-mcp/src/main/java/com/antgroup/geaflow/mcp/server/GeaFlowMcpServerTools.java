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

package com.antgroup.geaflow.mcp.server;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.antgroup.geaflow.analytics.service.client.AnalyticsClient;
import com.antgroup.geaflow.analytics.service.client.AnalyticsClientBuilder;
import com.antgroup.geaflow.analytics.service.query.QueryResults;
import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.mcp.util.YamlParser;
import java.util.Map;
import org.noear.solon.ai.annotation.ResourceMapping;
import org.noear.solon.ai.annotation.ToolMapping;
import org.noear.solon.ai.mcp.server.annotation.McpServerEndpoint;
import org.noear.solon.annotation.Param;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@McpServerEndpoint(name = "geaflow-mcp-server", sseEndpoint = "/geaflow/sse")
public class GeaFlowMcpServerTools {
    private static final Logger LOGGER = LoggerFactory.getLogger(GeaFlowMcpServerTools.class);

    private static final String RETRY_TIMES = "analytics.retry.times";
    private static final int DEFAULT_RETRY_TIMES = 3;
    private static final String ERROR = "error";
    public static final String SERVER_HOST = "analytics.server.host";
    public static final String SERVER_PORT = "analytics.server.port";
    public static final String SERVER_USER = "analytics.query.user";
    public static final String QUERY_TIMEOUT_MS = "analytics.query.timeout.ms";
    public static final String INIT_CHANNEL_POOLS = "analytics.init.channel.pools";
    public static final String CONFIG = "analytics.client.config";
    public static final String CURRENT_VERSION = "v1.0.0";

    /**
     * Resource that provides getting geaflow mcp server version.
     * @return version id.
     */
    @ResourceMapping(uri = "config://mcp-server-version", description = "Get mcp server version")
    public String getServerVersion() {
        return CURRENT_VERSION;
    }

    /**
     * A tool that provides graph query capabilities.
     * @param query GQL query.
     * @return query result or error code.
     */
    @ToolMapping(description = "execute query")
    public String executeQuery(@Param(name = "query", description = "query") String query) {
        AnalyticsClient analyticsClient = null;

        try {
            Map<String, Object> config = YamlParser.loadConfig();
            int retryTimes = DEFAULT_RETRY_TIMES;
            if (config.containsKey(RETRY_TIMES)) {
                retryTimes = Integer.parseInt(config.get(RETRY_TIMES).toString());
            }

            AnalyticsClientBuilder builder = AnalyticsClient
                .builder()
                .withHost(config.get(SERVER_HOST).toString())
                .withPort((Integer) config.get(SERVER_PORT))
                .withRetryNum(retryTimes);
            if (config.containsKey(CONFIG)) {
                Map<String, String> clientConfig = JSON.parseObject(config.get(CONFIG).toString(), Map.class);
                Configuration configuration = new Configuration(clientConfig);
                builder.withConfiguration(configuration);
                LOGGER.info("client config: {}", configuration);
            }
            if (config.containsKey(SERVER_USER)) {
                builder.withUser(config.get(SERVER_USER).toString());
            }
            if (config.containsKey(QUERY_TIMEOUT_MS)) {
                builder.withTimeoutMs((Integer) config.get(QUERY_TIMEOUT_MS));
            }
            if (config.containsKey(INIT_CHANNEL_POOLS)) {
                builder.withInitChannelPools((Boolean) config.get(INIT_CHANNEL_POOLS));
            }
            analyticsClient = builder.build();

            QueryResults queryResults = analyticsClient.executeQuery(query);
            if (queryResults.getError() != null) {
                final JSONObject error = new JSONObject();
                error.put(ERROR, queryResults.getError());
                return error.toJSONString();
            }
            return queryResults.getFormattedData();
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
            throw new RuntimeException(e);
        } finally {
            if (analyticsClient != null) {
                analyticsClient.shutdown();
            }
        }
    }
}
