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

import com.antgroup.geaflow.cluster.constants.AgentConstants;
import com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys;
import com.antgroup.geaflow.dashboard.agent.AgentWebServer;
import com.antgroup.geaflow.dashboard.agent.handler.LogRestHandler;
import org.eclipse.jetty.util.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AgentWebRunner {

    private static final Logger LOGGER = LoggerFactory.getLogger(LogRestHandler.class);

    public static void main(String[] args) {
        try {
            String httpPort = getPropertyOrDefault(AgentConstants.AGENT_SERVER_PORT_KEY,
                String.valueOf(ExecutionConfigKeys.AGENT_HTTP_PORT.getDefaultValue()));
            String agentDir = getPropertyOrDefault(AgentConstants.AGENT_TMP_DIR_KEY,
                String.valueOf(ExecutionConfigKeys.JOB_WORK_PATH.getDefaultValue()));
            String runtimeLogDirPath = getPropertyOrDefault(AgentConstants.LOG_DIR_KEY, null);
            String flameGraphProfilerPath = getPropertyOrDefault(AgentConstants.FLAME_GRAPH_PROFILER_PATH_KEY, null);
            AgentWebServer server = new AgentWebServer(Integer.parseInt(httpPort), runtimeLogDirPath, flameGraphProfilerPath, agentDir);
            server.start();
            server.await();
        } catch (Throwable t) {
            LOGGER.error("Agent start failed.", t);
            throw new RuntimeException(t);
        }
    }

    private static String getPropertyOrDefault(String key, String defaultValue) {
        String value = System.getProperty(key);
        if (StringUtil.isEmpty(value)) {
            LOGGER.info("Jvm property {} not found, using default value {}", key, defaultValue);
            return defaultValue;
        }
        return value;
    }

}
