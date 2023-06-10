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

import static com.antgroup.geaflow.cluster.k8s.utils.K8SConstants.CLIENT_NAME_SUFFIX;
import static com.antgroup.geaflow.cluster.k8s.utils.K8SConstants.SERVICE_NAME_SUFFIX;

import com.antgroup.geaflow.cluster.k8s.clustermanager.GeaflowKubeClient;
import com.antgroup.geaflow.cluster.k8s.clustermanager.KubernetesResourceBuilder;
import com.antgroup.geaflow.cluster.k8s.config.KubernetesClientParam;
import com.antgroup.geaflow.cluster.k8s.config.KubernetesConfig;
import com.antgroup.geaflow.cluster.k8s.config.KubernetesConfig.DockerNetworkType;
import com.antgroup.geaflow.cluster.k8s.utils.K8SConstants;
import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys;
import com.antgroup.geaflow.common.exception.GeaflowRuntimeException;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.OwnerReferenceBuilder;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.Service;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility to submit a job in cluster mode.
 * The config contains: clusterId, mainClass, clientArgs, and k8s related config.
 */
public class KubernetesJobClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(KubernetesJobClient.class);

    private final Configuration configuration;
    private final GeaflowKubeClient geaflowKubeClient;
    private final KubernetesClientParam clientParam;
    private final String clusterId;

    public KubernetesJobClient(Map<String, String> config, String masterUrl) {
        this(config, new GeaflowKubeClient(config, masterUrl));
    }

    @VisibleForTesting
    public KubernetesJobClient(Map<String, String> config, GeaflowKubeClient client) {
        this.configuration = new Configuration(config);
        this.clientParam = new KubernetesClientParam(configuration);
        this.geaflowKubeClient = client;
        this.clusterId = configuration.getString(ExecutionConfigKeys.CLUSTER_ID);
        Preconditions.checkArgument(StringUtils.isNotBlank(clusterId),
            "ClusterId is not set: " + ExecutionConfigKeys.CLUSTER_ID);
    }

    public void submitJob() {
        try {
            DockerNetworkType dockerNetworkType = KubernetesConfig
                .getDockerNetworkType(configuration);

            // create configMap.
            ConfigMap configMap = createConfigMap(clusterId);

            // create container
            String podName = clusterId + CLIENT_NAME_SUFFIX;
            Container container = KubernetesResourceBuilder
                .createContainer(podName, String.valueOf(0), null, clientParam,
                    clientParam.getContainerShellCommand(), clientParam.getAdditionEnvs(),
                    dockerNetworkType);

            // create ownerReference
            OwnerReference ownerReference = createOwnerReference(configMap);

            // create pod
            Pod pod = KubernetesResourceBuilder
                .createPod(clusterId, podName, podName, ownerReference, configMap, clientParam,
                    container);
            geaflowKubeClient.createPod(pod);
        } catch (Exception e) {
            LOGGER.error("Failed to create client pod:{}", e.getMessage(), e);
            throw new GeaflowRuntimeException(e);
        }
    }

    /**
     * Setup a Config Map that will generate a geaflow-conf.yaml and log4j file.
     * @param clusterId the cluster id
     * @return the created configMap
     */
    private ConfigMap createConfigMap(String clusterId) {
        ConfigMap configMap = KubernetesResourceBuilder.createConfigMap(clusterId, clientParam,
            null);
        return geaflowKubeClient.createConfigMap(configMap);
    }

    private OwnerReference createOwnerReference(ConfigMap configMap) {
        Preconditions.checkNotNull(configMap, "configMap could not be null");
        return new OwnerReferenceBuilder()
            .withName(configMap.getMetadata().getName())
            .withApiVersion(configMap.getApiVersion())
            .withUid(configMap.getMetadata().getUid())
            .withKind(configMap.getKind())
            .withController(true)
            .build();
    }

    public void stopJob() {
        geaflowKubeClient.destroyCluster(clusterId);
        String clientConfigMap = clientParam.getConfigMapName(clusterId);
        geaflowKubeClient.deleteConfigMap(clientConfigMap);
    }

    public Service getMasterService() {
        String masterServiceName = clusterId + SERVICE_NAME_SUFFIX;
        return geaflowKubeClient.getService(masterServiceName);
    }

    public List<Pod> getJobPods() {
        Map<String, String> podLabels = new HashMap<>();
        podLabels.put(K8SConstants.LABEL_APP_KEY, clusterId);
        return geaflowKubeClient.getPods(podLabels).getItems();
    }

}
