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

import static com.antgroup.geaflow.cluster.k8s.config.KubernetesConfigKeys.CLUSTER_FAULT_INJECTION_ENABLE;
import static com.antgroup.geaflow.cluster.k8s.config.KubernetesConfigKeys.CONF_DIR;
import static com.antgroup.geaflow.cluster.k8s.config.KubernetesConfigKeys.CONTAINER_IMAGE;
import static com.antgroup.geaflow.cluster.k8s.config.KubernetesConfigKeys.CONTAINER_IMAGE_PULL_POLICY;
import static com.antgroup.geaflow.cluster.k8s.config.KubernetesConfigKeys.CONTAINER_START_COMMAND_TEMPLATE;
import static com.antgroup.geaflow.cluster.k8s.config.KubernetesConfigKeys.LOG_DIR;
import static com.antgroup.geaflow.cluster.k8s.config.KubernetesConfigKeys.PROCESS_AUTO_RESTART;
import static com.antgroup.geaflow.cluster.k8s.config.KubernetesConfigKeys.SERVICE_ACCOUNT;
import static com.antgroup.geaflow.cluster.k8s.config.KubernetesConfigKeys.SERVICE_USER_ANNOTATIONS;
import static com.antgroup.geaflow.cluster.k8s.config.KubernetesConfigKeys.SERVICE_USER_LABELS;

import com.antgroup.geaflow.cluster.config.ClusterConfig;
import com.antgroup.geaflow.cluster.config.ClusterJvmOptions;
import com.antgroup.geaflow.cluster.k8s.utils.K8SConstants;
import com.antgroup.geaflow.cluster.k8s.utils.KubernetesUtils;
import com.antgroup.geaflow.common.config.Configuration;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.QuantityBuilder;
import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;

public abstract class AbstractKubernetesParam implements KubernetesParam {

    protected Configuration config;
    protected ClusterConfig clusterConfig;

    protected static final String DEFAULT_AUTO_RESTART = "unexpected";

    public AbstractKubernetesParam(Configuration config) {
        this.config = config;
        this.clusterConfig = ClusterConfig.build(config);
    }

    public AbstractKubernetesParam(ClusterConfig clusterConfig) {
        this.clusterConfig = clusterConfig;
        this.config = clusterConfig.getConfig();
    }

    public String getContainerImage() {
        return config.getString(CONTAINER_IMAGE);
    }

    public String getContainerImagePullPolicy() {
        return config.getString(CONTAINER_IMAGE_PULL_POLICY);
    }

    public String getServiceName(String clusterId) {
        return clusterId + K8SConstants.SERVICE_NAME_SUFFIX;
    }

    public String getServiceAccount() {
        return config.getString(SERVICE_ACCOUNT);
    }

    public Map<String, String> getServiceLabels() {
        return KubernetesUtils.getPairsConf(config, SERVICE_USER_LABELS.getKey());
    }

    @Override
    public Map<String, String> getServiceAnnotations() {
        return KubernetesUtils.getPairsConf(config, SERVICE_USER_ANNOTATIONS.getKey());
    }

    public String getContainerStartCommandTemplate() {
        return config.getString(CONTAINER_START_COMMAND_TEMPLATE);
    }

    protected Quantity getCpuQuantity(double cpu) {
        return new QuantityBuilder(false).withAmount(String.valueOf(cpu)).build();
    }

    protected Quantity getMemoryQuantity(long memoryMB) {
        return new QuantityBuilder(false).withAmount(String.valueOf((memoryMB) << 20))
            .build();
    }

    protected Quantity getDiskQuantity(long diskGB) {
        return new QuantityBuilder(false).withAmount(String.valueOf((diskGB) << 30))
            .build();
    }

    @Override
    public String getConfDir() {
        return config.getString(CONF_DIR);
    }

    @Override
    public String getLogDir() {
        return config.getString(LOG_DIR);
    }

    @Override
    public String getAutoRestart() {
        return config.getString(PROCESS_AUTO_RESTART);
    }

    @Override
    public Boolean getClusterFaultInjectionEnable() {
        return config.getBoolean(CLUSTER_FAULT_INJECTION_ENABLE);
    }

    @Override
    public int getRpcPort() {
        return 0;
    }

    @Override
    public int getHttpPort() {
        return 0;
    }

    public int getNodePort() {
        return 0;
    }

    @Override
    public Quantity getCpuQuantity() {
        Double cpu = getContainerCpu();
        return getCpuQuantity(cpu);
    }

    protected abstract Double getContainerCpu();

    @Override
    public Quantity getMemoryQuantity() {
        long memoryMB = getContainerMemoryMB();
        return getMemoryQuantity(memoryMB);
    }

    protected abstract long getContainerMemoryMB();

    @Override
    public Quantity getDiskQuantity() {
        long diskGB = getContainerDiskGB();
        if (diskGB == 0) {
            return null;
        }
        return getDiskQuantity(diskGB);
    }

    protected abstract long getContainerDiskGB();

    @Override
    public Map<String, String> getAdditionEnvs() {
        return null;
    }

    @Override
    public Configuration getConfig() {
        return config;
    }

    /**
     * This method is an adaptation of Flink's.
     * org.apache.flink.runtime.clusterframework.BootstrapTools#getTaskManagerShellCommand.
     */
    public String getContainerShellCommand(ClusterJvmOptions jvmOpts, Class<?> mainClass,
                                           String logFilename) {
        String confDir = getConfDir();
        final Map<String, String> startCommandValues = new HashMap<>();
        startCommandValues.put("java", "$JAVA_HOME/bin/java");
        startCommandValues.put("classpath",
            "-classpath " + confDir + File.pathSeparator + "$" + K8SConstants.ENV_GEAFLOW_CLASSPATH);
        startCommandValues.put("class", mainClass.getName());

        ArrayList<String> params = new ArrayList<>();
        params.add(String.format("-Xms%dm", jvmOpts.getXmsMB()));
        params.add(String.format("-Xmx%dm", jvmOpts.getMaxHeapMB()));
        if (jvmOpts.getXmnMB() > 0) {
            params.add(String.format("-Xmn%dm", jvmOpts.getXmnMB()));
        }
        if (jvmOpts.getMaxDirectMB() > 0) {
            params.add(String.format("-XX:MaxDirectMemorySize=%dm", jvmOpts.getMaxDirectMB()));
        }
        startCommandValues.put("jvmmem", StringUtils.join(params, ' '));
        startCommandValues.put("jvmopts", StringUtils.join(jvmOpts.getExtraOptions(), ' '));

        StringBuilder logging = new StringBuilder();
        logging.append("-Dlog.file=\"").append(logFilename).append("\"");

        startCommandValues.put("logging", logging.toString());
        startCommandValues.put("redirects", ">> " + logFilename + " 2>&1");

        String commandTemplate = getContainerStartCommandTemplate();
        return KubernetesUtils.getContainerStartCommand(commandTemplate, startCommandValues);
    }
}
