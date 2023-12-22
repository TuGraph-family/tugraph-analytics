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

import static com.antgroup.geaflow.cluster.constants.ClusterConstants.MASTER_LOG_SUFFIX;
import static com.antgroup.geaflow.cluster.k8s.config.K8SConstants.JOB_CLASSPATH;
import static com.antgroup.geaflow.cluster.k8s.config.KubernetesConfigKeys.POD_USER_LABELS;
import static com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys.MASTER_HTTP_PORT;

import com.antgroup.geaflow.cluster.config.ClusterConfig;
import com.antgroup.geaflow.cluster.k8s.entrypoint.KubernetesMasterRunner;
import com.antgroup.geaflow.cluster.k8s.utils.KubernetesUtils;
import com.antgroup.geaflow.cluster.runner.failover.AutoRestartPolicy;
import com.antgroup.geaflow.cluster.runner.util.ClusterUtils;
import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys;
import java.io.File;
import java.util.HashMap;
import java.util.Map;

public class KubernetesMasterParam extends AbstractKubernetesParam {

    public static final String MASTER_CONTAINER_NAME = "kubernetes.master.container.name";

    public static final String MASTER_USER_ANNOTATIONS = "kubernetes.master.user.annotations";

    public static final String MASTER_NODE_SELECTOR = "kubernetes.master.node-selector";

    public static final String MASTER_ENV_PREFIX = "kubernetes.master.env.";


    public KubernetesMasterParam(Configuration config) {
        super(config);
    }

    public KubernetesMasterParam(ClusterConfig config) {
        super(config);
    }

    @Override
    public String getAutoRestart() {
        return AutoRestartPolicy.UNEXPECTED.getValue();
    }

    public String getContainerName() {
        return config.getString(MASTER_CONTAINER_NAME, "geaflow-master");
    }

    public Double getContainerCpu() {
        return clusterConfig.getMasterVcores();
    }

    @Override
    public long getContainerMemoryMB() {
        return clusterConfig.getMasterMemoryMB();
    }

    @Override
    protected long getContainerDiskGB() {
        return clusterConfig.getMasterDiskGB();
    }

    @Override
    public String getContainerShellCommand() {
        String logFilename = getLogDir() + File.separator + MASTER_LOG_SUFFIX;
        return ClusterUtils.getStartCommand(clusterConfig.getMasterJvmOptions(),
            KubernetesMasterRunner.class, logFilename, config, JOB_CLASSPATH);
    }

    @Override
    public String getPodNamePrefix(String clusterId) {
        return clusterId + K8SConstants.MASTER_NAME_SUFFIX + K8SConstants.NAME_SEPARATOR;
    }

    @Override
    public int getHttpPort() {
        return config.getInteger(MASTER_HTTP_PORT);
    }

    @Override
    public Map<String, String> getAdditionEnvs() {
        return KubernetesUtils
            .getVariablesWithPrefix(MASTER_ENV_PREFIX, config.getConfigMap());
    }

    @Override
    public String getConfigMapName(String clusterId) {
        return clusterId + K8SConstants.MASTER_CONFIG_MAP_SUFFIX;
    }

    @Override
    public Map<String, String> getPodLabels(String clusterId) {
        Map<String, String> labels = new HashMap<>();
        labels.put(K8SConstants.LABEL_APP_KEY, clusterId);
        labels.put(K8SConstants.LABEL_COMPONENT_KEY, K8SConstants.LABEL_COMPONENT_MASTER);
        labels.putAll(KubernetesUtils.getPairsConf(config, POD_USER_LABELS));
        return labels;
    }

    @Override
    public Map<String, String> getAnnotations() {
        return KubernetesUtils.getPairsConf(config, MASTER_USER_ANNOTATIONS);
    }

    @Override
    public Map<String, String> getNodeSelector() {
        return KubernetesUtils.getPairsConf(config, MASTER_NODE_SELECTOR);
    }

    @Override
    public boolean enableLeaderElection() {
        return config.getBoolean(ExecutionConfigKeys.ENABLE_MASTER_LEADER_ELECTION);
    }
}
