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

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.noear.solon.ai.mcp.client.McpClientProvider;
import org.noear.solon.test.SolonTest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SolonTest(GeaFlowMcpServer.class)
public class GeaFlowMcpClientTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(GeaFlowMcpClientTest.class);

    private static final String SSE_ENDPOINT = "http://localhost:8088/geaflow/sse";
    private static final String QUERY = "query";
    private static final String EXECUTE_QUERY_TOOL_NAME = "executeQuery";
    private static final String GRAPH_G1 = "create graph g1("
        + "vertex user("
        + " id bigint ID,"
        + "name varchar"
        + "),"
        + "vertex person("
        + " id bigint ID,"
        + "name varchar,"
        + "gender int,"
        + "age integer"
        + "),"
        + "edge knows("
        + " src_id bigint SOURCE ID,"
        + " target_id bigint DESTINATION ID,"
        + " time bigint TIMESTAMP,"
        + " weight double"
        + ")"
        + ")";
    private static final String EXECUTE_QUERY = GRAPH_G1 + "match(a:user)-[e:knows]->(b:user)";

    /**
     * Call execute query tool.
     */
    @Test
    public void testExecuteQuery() {
        McpClientProvider toolProvider = McpClientProvider.builder()
            .apiUrl(SSE_ENDPOINT)
            .build();

        Map<String, Object> map = Collections.singletonMap(QUERY, EXECUTE_QUERY);
        String queryResults = toolProvider.callToolAsText(EXECUTE_QUERY_TOOL_NAME, map).getContent();
        LOGGER.info("queryResults: {}", queryResults);
        Assertions.assertEquals("{\"error\":{\"code\":1001,\"name\":\"ANALYTICS_SERVER_UNAVAILABLE\"}}",
            queryResults);

    }

    @Test
    public void testGetServerVersion() {
        McpClientProvider toolProvider = McpClientProvider.builder()
            .apiUrl(SSE_ENDPOINT)
            .build();

        String resourceContent = toolProvider.readResourceAsText("config://mcp-server-version").getContent();
        Assertions.assertTrue(resourceContent.contains("v1.0.0"));
    }

    @Test
    public void testListTools() {
        McpClientProvider toolProvider = McpClientProvider.builder()
            .apiUrl(SSE_ENDPOINT)
            .build();

        Set<String> toolNames = new HashSet<>();
        toolProvider.getTools().stream().forEach(tool -> {
            LOGGER.info("Tool: {}, desc: {}, input schema: {}", tool.name(), tool.description(), tool.inputSchema());
            toolNames.add(tool.name());
        });
        Assertions.assertTrue(toolNames.contains(EXECUTE_QUERY_TOOL_NAME));
    }

}
