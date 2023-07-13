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

package com.antgroup.geaflow.dsl.runtime.engine;

import static com.antgroup.geaflow.cluster.constants.ClusterConstants.CLUSTER_TYPE;

import com.antgroup.geaflow.env.Environment;
import com.antgroup.geaflow.env.EnvironmentFactory;
import com.antgroup.geaflow.env.IEnvironment.EnvType;
import com.antgroup.geaflow.file.FileConfigKeys;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GeaFlowGqlClient {

    private static final Logger LOGGER = LoggerFactory.getLogger("GeaFlowGqlClient");

    private static final String CONF_FILE_NAME = "user.conf";

    public static void main(String[] args) throws Exception {
        for (int i = 0; i < args.length; i++) {
            LOGGER.info("args[{}]: {}", i, args[i]);
        }
        Environment environment = loadEnvironment(args);
        Map<String, Integer> parallelismConfigMap = loadParallelismConfig();
        LOGGER.info("parallelism config map: {}", parallelismConfigMap);
        int timeWait = -1; // No wait for remote mode.
        if (environment.getEnvType() == EnvType.LOCAL) {
            timeWait = 0; // Infinite wait for local test.
            Map<String, String> localConfig = new HashMap<>();
            localConfig.put(FileConfigKeys.ROOT.getKey(), "/tmp/dsl/");
            environment.getEnvironmentContext().withConfig(localConfig);
        }
        GQLPipeLine pipeLine = new GQLPipeLine(environment, timeWait, parallelismConfigMap);
        pipeLine.execute();
    }

    private static Map<String, Integer> loadParallelismConfig() {
        try {
            String parallelismConf = IOUtils.resourceToString(CONF_FILE_NAME,
                Charset.defaultCharset(), GeaFlowGqlClient.class.getClassLoader());
            Gson gson = new Gson();
            return gson.fromJson(parallelismConf, new TypeToken<Map<String,
                Integer>>() {
            }.getType());
        } catch (IOException e) {
            if (!e.getMessage().contains("Resource not found")) {
                LOGGER.warn("Error in load parallelism config file", e);
            }
            return new HashMap<>();
        }
    }

    private static EnvType getClusterType() {
        String clusterType = System.getProperty(CLUSTER_TYPE);
        if (StringUtils.isBlank(clusterType)) {
            LOGGER.warn("use LOCAL as default cluster");
            return EnvType.LOCAL;
        }
        return (EnvType.valueOf(clusterType.toUpperCase(Locale.ROOT)));
    }

    public static Environment loadEnvironment(String[] args) {
        EnvType clusterType = getClusterType();
        switch (clusterType) {
            case K8S:
                return EnvironmentFactory.onK8SEnvironment(args);
            case RAY_COMMUNITY:
                return EnvironmentFactory.onRayCommunityEnvironment(args);
            default:
                return EnvironmentFactory.onLocalEnvironment(args);
        }
    }
}
