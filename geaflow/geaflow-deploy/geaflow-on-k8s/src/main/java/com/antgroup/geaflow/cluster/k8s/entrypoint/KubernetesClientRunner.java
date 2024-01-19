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

package com.antgroup.geaflow.cluster.k8s.entrypoint;

import static com.antgroup.geaflow.cluster.constants.ClusterConstants.CLUSTER_TYPE;
import static com.antgroup.geaflow.cluster.k8s.config.KubernetesConfigKeys.USER_CLASS_ARGS;
import static com.antgroup.geaflow.cluster.k8s.config.KubernetesConfigKeys.USER_MAIN_CLASS;
import static com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys.CLIENT_EXIT_WAIT_SECONDS;

import com.antgroup.geaflow.cluster.client.callback.ClusterCallbackFactory;
import com.antgroup.geaflow.cluster.client.callback.ClusterStartedCallback;
import com.antgroup.geaflow.cluster.k8s.clustermanager.GeaflowKubeClient;
import com.antgroup.geaflow.cluster.k8s.config.KubernetesClientParam;
import com.antgroup.geaflow.cluster.k8s.config.KubernetesConfig;
import com.antgroup.geaflow.cluster.k8s.utils.KubernetesUtils;
import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys;
import com.antgroup.geaflow.common.exception.GeaflowRuntimeException;
import com.antgroup.geaflow.common.utils.SleepUtils;
import com.antgroup.geaflow.env.IEnvironment.EnvType;
import java.io.IOException;
import java.lang.reflect.Method;
import org.apache.commons.lang3.StringEscapeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KubernetesClientRunner {

    private static final Logger LOGGER = LoggerFactory.getLogger(KubernetesClientRunner.class);
    private final Configuration config;

    public KubernetesClientRunner(Configuration config) {
        this.config = config;
    }

    public void run(String classArgs) {
        String userClass = null;
        ClusterStartedCallback callback = ClusterCallbackFactory.createClusterStartCallback(config);
        try {
            System.setProperty(CLUSTER_TYPE, EnvType.K8S.name());
            userClass = config.getString(USER_MAIN_CLASS);
            Class<?> mainClazz = Thread.currentThread().getContextClassLoader().loadClass(userClass);
            Method mainMethod = mainClazz.getMethod("main", String[].class);
            LOGGER.info("execute mainClass {} to k8s, params: {}", userClass, classArgs);
            mainMethod.invoke(mainClazz, (Object) new String[] {classArgs});
        } catch (Throwable e) {
            LOGGER.error("execute mainClass {} failed: {}", userClass, e.getMessage());
            callback.onFailure(e);
            throw new GeaflowRuntimeException(e);
        } finally {
            cleanAndExit();
        }
    }

    public static void main(String[] args) throws IOException {
        try {
            final long startTime = System.currentTimeMillis();
            Configuration config = KubernetesUtils.loadConfigurationFromFile();
            final String classArgs = StringEscapeUtils.escapeJava(config.getString(USER_CLASS_ARGS));
            KubernetesClientRunner clientRunner = new KubernetesClientRunner(config);
            clientRunner.run(classArgs);
            LOGGER.info("Completed client init in {} ms", System.currentTimeMillis() - startTime);
        } catch (Throwable e) {
            LOGGER.error("init client runner failed: {}", e.getMessage(), e);
            throw e;
        }
    }

    private void cleanAndExit() {
        try {
            int waitTime = config.getInteger(CLIENT_EXIT_WAIT_SECONDS);
            LOGGER.info("Sleep {} seconds before client exits...", waitTime);
            SleepUtils.sleepSecond(waitTime);
            deleteClientConfigMap();
        } catch (Throwable e) {
            LOGGER.error("delete client config map failed: {}", e.getMessage(), e);
        }
    }

    private void deleteClientConfigMap() {
        String clusterId = config.getString(ExecutionConfigKeys.CLUSTER_ID);
        String masterUrl = KubernetesConfig.getClientMasterUrl(config);
        KubernetesClientParam clientParam = new KubernetesClientParam(config);
        String clientConfigMap = clientParam.getConfigMapName(clusterId);
        GeaflowKubeClient kubernetesClient = new GeaflowKubeClient(config, masterUrl);
        kubernetesClient.deleteConfigMap(clientConfigMap);
    }

}
