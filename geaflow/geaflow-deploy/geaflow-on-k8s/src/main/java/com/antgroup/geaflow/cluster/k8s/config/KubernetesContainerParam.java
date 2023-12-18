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

import static com.antgroup.geaflow.cluster.constants.ClusterConstants.CONTAINER_LOG_SUFFIX;
import static com.antgroup.geaflow.cluster.k8s.config.K8SConstants.JOB_CLASSPATH;
import static com.antgroup.geaflow.cluster.k8s.config.KubernetesConfigKeys.POD_USER_LABELS;

import com.antgroup.geaflow.cluster.config.ClusterConfig;
import com.antgroup.geaflow.cluster.k8s.utils.KubernetesUtils;
import com.antgroup.geaflow.cluster.runner.entrypoint.ContainerRunner;
import com.antgroup.geaflow.cluster.runner.util.ClusterUtils;
import com.antgroup.geaflow.common.config.Configuration;
import java.io.File;
import java.util.HashMap;
import java.util.Map;

public class KubernetesContainerParam extends AbstractKubernetesParam {

    public static final String CONTAINER_USER_ANNOTATIONS = "kubernetes.container.user.annotations";

    public static final String CONTAINER_NODE_SELECTOR = "kubernetes.container.node-selector";

    public static final String CONTAINER_ENV_PREFIX = "kubernetes.container.env.";

    public KubernetesContainerParam(Configuration config) {
        super(config);
    }

    public KubernetesContainerParam(ClusterConfig config) {
        super(config);
    }

    @Override
    public Double getContainerCpu() {
        return clusterConfig.getContainerVcores();
    }

    @Override
    public long getContainerMemoryMB() {
        return clusterConfig.getContainerMemoryMB();
    }

    @Override
    protected long getContainerDiskGB() {
        return clusterConfig.getContainerDiskGB();
    }

    @Override
    public String getContainerShellCommand() {
        String logFilename = getLogDir() + File.separator + CONTAINER_LOG_SUFFIX;
        return ClusterUtils.getStartCommand(clusterConfig.getContainerJvmOptions(),
            ContainerRunner.class, logFilename, config, JOB_CLASSPATH);
    }

    @Override
    public Map<String, String> getAdditionEnvs() {
        return KubernetesUtils
            .getVariablesWithPrefix(CONTAINER_ENV_PREFIX, config.getConfigMap());
    }

    @Override
    public String getPodNamePrefix(String clusterId) {
        return clusterId + K8SConstants.WORKER_NAME_SUFFIX + K8SConstants.NAME_SEPARATOR;
    }

    @Override
    public String getConfigMapName(String clusterId) {
        return clusterId + K8SConstants.WORKER_CONFIG_MAP_SUFFIX;
    }

    @Override
    public Map<String, String> getPodLabels(String clusterId) {
        Map<String, String> workerPodLabels = new HashMap<>();
        workerPodLabels.put(K8SConstants.LABEL_APP_KEY, clusterId);
        workerPodLabels.put(K8SConstants.LABEL_COMPONENT_KEY, K8SConstants.LABEL_COMPONENT_WORKER);
        workerPodLabels.putAll(KubernetesUtils.getPairsConf(config, POD_USER_LABELS));
        return workerPodLabels;
    }

    @Override
    public Map<String, String> getAnnotations() {
        return KubernetesUtils.getPairsConf(config, CONTAINER_USER_ANNOTATIONS);
    }

    @Override
    public Map<String, String> getNodeSelector() {
        return KubernetesUtils.getPairsConf(config, CONTAINER_NODE_SELECTOR);
    }
}
