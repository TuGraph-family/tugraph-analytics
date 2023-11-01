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

package com.antgroup.geaflow.dashboard.agent.runner;

import com.antgroup.geaflow.cluster.k8s.config.K8SConstants;
import com.antgroup.geaflow.common.exception.GeaflowRuntimeException;
import com.antgroup.geaflow.dashboard.agent.AgentWebServer;
import com.antgroup.geaflow.dashboard.agent.handler.LogRestHandler;
import org.eclipse.jetty.util.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AgentWebRunner {

    private static final Logger LOGGER = LoggerFactory.getLogger(LogRestHandler.class);

    private static final String FLAME_GRAPH_PROFILER_PATH = "FLAME_GRAPH_PROFILER_PATH";

    private static final String AGENT_TMP_DIR = "AGENT_TMP_DIR";

    public static void main(String[] args) {
        try {
            String httpPort = getProperty(K8SConstants.ENV_AGENT_SERVER_PORT);
            String deployLogPath = getProperty(K8SConstants.ENV_DEPLOY_LOG_PATH);
            String runtimeLogDirPath = getProperty(K8SConstants.ENV_LOG_DIR);
            String flameGraphProfilerPath = getProperty(FLAME_GRAPH_PROFILER_PATH);
            String agentDir = getProperty(AGENT_TMP_DIR);
            AgentWebServer server = new AgentWebServer(Integer.parseInt(httpPort), deployLogPath,
                runtimeLogDirPath, flameGraphProfilerPath, agentDir);
            server.start();
            server.await();
        } catch (Throwable t) {
            LOGGER.error("Agent start failed.", t);
            throw new RuntimeException(t);
        }
    }

    private static String getProperty(String key) {
        String value = System.getProperty(key);
        if (StringUtil.isEmpty(value)) {
            throw new GeaflowRuntimeException(String.format("Jvm property %s not found.", key));
        }
        return value;
    }

}
