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

import com.antgroup.geaflow.common.config.Configuration;
import io.fabric8.kubernetes.api.model.Quantity;
import java.util.Map;

/**
 * A collection of Kubernetes parameters for pod creating.
 * This interface is an adaptation of Flink's org.apache.flink.kubernetes.kubeclient.parameters.KubernetesParameters.
 */
public interface KubernetesParam {

    /**
     * Get service account.
     */
    String getServiceAccount();

    /**
     * Get service name.
     */
    String getServiceName(String clusterId);

    /**
     * Get all key-value pair labels for service.
     */
    Map<String, String> getServiceLabels();

    /**
     * Get all key-value pair annotations for service.
     */
    Map<String, String> getServiceAnnotations();

    /**
     * Get all key-value pair labels for pod.
     * @param clusterId Current k8s cluster id.
     */
    Map<String, String> getPodLabels(String clusterId);

    /**
     * Get all node selectors.
     */
    Map<String, String> getNodeSelector();

    /**
     * Get all annotations shared for all pods.
     */
    Map<String, String> getAnnotations();

    /**
     * Get container image name.
     */
    String getContainerImage();

    /**
     * Get container image pull policy.
     */
    String getContainerImagePullPolicy();

    /**
     * Get the shell command of start container process.
     */
    String getContainerShellCommand();

    /**
     * Get pod name prefix of current cluster.
     */
    String getPodNamePrefix(String clusterId);

    /**
     * Get the name of configuration for current cluster.
     */
    String getConfigMapName(String clusterId);

    /**
     * Get the value of cpu request for pod.
     */
    Quantity getCpuQuantity();

    /**
     * Get the value of memory request for pod.
     */
    Quantity getMemoryQuantity();

    /**
     * Get the value of disk request for pod.
     */
    Quantity getDiskQuantity();

    /**
     * Get current exposed rpc port.
     */
    int getRpcPort();

    /**
     * Get current exposed http port.
     */
    int getHttpPort();

    /**
     * Get current exposed node port.
     */
    int getNodePort();

    /**
     * Get env config directory.
     */
    String getConfDir();

    /**
     * Get log directory.
     */
    String getLogDir();

    /**
     * Get the flag that process should auto start after crashed.
     */
    String getAutoRestart();

    /**
     * Get the flag whether allow injecting error or exception.
     *
     */
    Boolean getClusterFaultInjectionEnable();

    /**
     * Get origin user configuration.
     */
    Configuration getConfig();

    /**
     * Get addition env from client.
     */
    Map<String, String> getAdditionEnvs();

}
