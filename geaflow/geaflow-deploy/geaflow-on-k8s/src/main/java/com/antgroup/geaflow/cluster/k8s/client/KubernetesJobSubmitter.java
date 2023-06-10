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

package com.antgroup.geaflow.cluster.k8s.client;

import static com.antgroup.geaflow.cluster.constants.ClusterConstants.CLUSTER_TYPE;
import static com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys.CLUSTER_ID;

import com.alibaba.fastjson.JSON;
import com.antgroup.geaflow.cluster.k8s.clustermanager.GeaflowKubeClient;
import com.antgroup.geaflow.cluster.k8s.config.KubernetesConfig;
import com.antgroup.geaflow.cluster.k8s.utils.KubernetesUtils;
import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.env.IEnvironment.EnvType;
import com.antgroup.geaflow.env.args.EnvironmentArgumentParser;
import com.google.common.base.Preconditions;
import java.lang.reflect.InvocationTargetException;
import java.util.Map;
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility to submit a job via shell scripts.
 */
public class KubernetesJobSubmitter {
    private static final Logger LOGGER = LoggerFactory.getLogger(KubernetesJobSubmitter.class);
    private static final String START_ACTION = "start";
    private static final String STOP_ACTION = "stop";

    public static void main(String[] args) throws Throwable {
        if (args.length < 1) {
            throw new IllegalArgumentException("usage: start/stop [mainClassName] [args]");
        }
        String action = args[0];
        KubernetesJobSubmitter submitter = new KubernetesJobSubmitter();
        if (action.equalsIgnoreCase(START_ACTION)) {
            submitter.submitJob(args);
        } else if (action.equalsIgnoreCase(STOP_ACTION)) {
            submitter.stopJob(args);
        } else {
            throw new IllegalArgumentException("unknown action:" + action);
        }
    }

    public void submitJob(String[] args) throws Throwable {
        if (args.length < 2) {
            throw new IllegalArgumentException("usage: start mainClassName [args]");
        }
        try {
            String driverArgs;
            String className = args[1];
            if (args.length > 2) {
                driverArgs = args[2];
            } else {
                Configuration config = KubernetesUtils.loadConfigurationFromFile();
                driverArgs = StringEscapeUtils.escapeJava(JSON.toJSONString(config.getConfigMap()));
            }
            LOGGER.info("{} driverArgs: {}", className, driverArgs);

            Class<?> clazz = Class.forName(className);
            System.setProperty(CLUSTER_TYPE, EnvType.K8S.name());
            clazz.getMethod("main", String[].class).invoke(null, (Object) new String[]{driverArgs});
        } catch (Throwable e) {
            if (e instanceof InvocationTargetException && e.getCause() != null) {
                e = e.getCause();
            }
            LOGGER.error("launch main failed", e);
            throw e;
        }
    }

    public void stopJob(String[] args) throws Throwable {
        Configuration configuration;
        GeaflowKubeClient client = null;
        try {
            if (args.length > 1) {
                EnvironmentArgumentParser parser = new EnvironmentArgumentParser();
                Map<String, String> config = parser.parse(new String[]{args[1]});
                configuration = new Configuration(config);
            } else {
                configuration = KubernetesUtils.loadConfigurationFromFile();
            }
            String masterUrl = KubernetesConfig.getClientMasterUrl(configuration);
            client = new GeaflowKubeClient(configuration, masterUrl);
            String clusterId = configuration.getString(CLUSTER_ID);
            Preconditions.checkArgument(StringUtils.isNotEmpty(clusterId), "clusterId is not set");
            LOGGER.info("stop job with cluster id:{}", clusterId);
            client.destroyCluster(clusterId);
        } catch (Throwable e) {
            if (e instanceof InvocationTargetException && e.getCause() != null) {
                e = e.getCause();
            }
            LOGGER.error("stop job failed", e);
            throw e;
        } finally {
            if (client != null) {
                client.close();
            }
        }
    }

}

