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

package com.antgroup.geaflow.cluster.k8s.config;

import static com.antgroup.geaflow.cluster.k8s.config.KubernetesConfigKeys.POD_USER_LABELS;

import com.antgroup.geaflow.cluster.k8s.entrypoint.KubernetesClientRunner;
import com.antgroup.geaflow.cluster.k8s.utils.K8SConstants;
import com.antgroup.geaflow.cluster.k8s.utils.KubernetesUtils;
import com.antgroup.geaflow.common.config.Configuration;
import java.io.File;
import java.util.HashMap;
import java.util.Map;

public class KubernetesClientParam extends AbstractKubernetesParam {

    public static final String CLIENT_USER_ANNOTATIONS = "kubernetes.client.user.annotations";

    public static final String CLIENT_NODE_SELECTOR = "kubernetes.client.node-selector";

    public static final String CONTAINERIZED_CLIENT_ENV_PREFIX = "containerized.client.env.";

    public static final String CLIENT_LOG_SUFFIX = "client.log";

    private static final String CLIENT_AUTO_RESTART = "false";

    public KubernetesClientParam(Configuration config) {
        super(config);
    }

    @Override
    public Double getContainerCpu() {
        return clusterConfig.getClientVcores();
    }

    @Override
    public long getContainerMemoryMB() {
        return clusterConfig.getClientMemoryMB();
    }

    @Override
    protected long getContainerDiskGB() {
        return clusterConfig.getClientDiskGB();
    }

    @Override
    public String getContainerShellCommand() {
        String logFilename = getLogDir() + File.separator + CLIENT_LOG_SUFFIX;
        return getContainerShellCommand(clusterConfig.getClientJvmOptions(),
            KubernetesClientRunner.class, logFilename);
    }

    @Override
    public Map<String, String> getAdditionEnvs() {
        return KubernetesUtils
            .getVariablesWithPrefix(CONTAINERIZED_CLIENT_ENV_PREFIX, config.getConfigMap());
    }

    @Override
    public String getPodNamePrefix(String clusterId) {
        return clusterId + K8SConstants.CLIENT_NAME_SUFFIX + K8SConstants.NAME_SEPARATOR;
    }

    @Override
    public String getConfigMapName(String clusterId) {
        return clusterId + K8SConstants.CLIENT_CONFIG_MAP_SUFFIX;
    }

    @Override
    public Map<String, String> getPodLabels(String clusterId) {
        Map<String, String> workerPodLabels = new HashMap<>();
        workerPodLabels.put(K8SConstants.LABEL_APP_KEY, clusterId);
        workerPodLabels.put(K8SConstants.LABEL_COMPONENT_KEY, K8SConstants.LABEL_COMPONENT_CLIENT);
        workerPodLabels.putAll(KubernetesUtils.getPairsConf(config, POD_USER_LABELS.getKey()));
        return workerPodLabels;
    }

    @Override
    public Map<String, String> getAnnotations() {
        return KubernetesUtils.getPairsConf(config, CLIENT_USER_ANNOTATIONS);
    }

    @Override
    public Map<String, String> getNodeSelector() {
        return KubernetesUtils.getPairsConf(config, CLIENT_NODE_SELECTOR);
    }

    @Override
    public String getAutoRestart() {
        return CLIENT_AUTO_RESTART;
    }
}
