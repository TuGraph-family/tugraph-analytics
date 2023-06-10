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

package com.antgroup.geaflow.cluster.k8s.clustermanager;

import com.antgroup.geaflow.cluster.k8s.config.KubernetesConfig;
import com.antgroup.geaflow.cluster.k8s.config.KubernetesMasterParam;
import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.utils.RetryCommand;
import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodList;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.FilterWatchListDeletable;
import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.Callable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Geaflow Kubernetes client to interact with kubernetes api server.
 */
public class GeaflowKubeClient implements Serializable {

    private static final Logger LOGGER = LoggerFactory.getLogger(GeaflowKubeClient.class);

    private final KubernetesMasterParam masterParam;
    private final KubernetesClient kubernetesClient;

    private final int retryCount;
    private final long retryInterval;

    public GeaflowKubeClient(Configuration config) {
        this(KubernetesClientFactory.create(config), config);
    }

    public GeaflowKubeClient(Map<String, String> config, String masterUrl) {
        this(new Configuration(config), masterUrl);
    }

    public GeaflowKubeClient(Configuration config, String masterUrl) {
        this(KubernetesClientFactory.create(config, masterUrl), config);
    }

    public GeaflowKubeClient(KubernetesClient client, Configuration config) {
        this.kubernetesClient = client;
        this.masterParam = new KubernetesMasterParam(config);
        this.retryCount = KubernetesConfig.getConnectionRetryTimes(config);
        this.retryInterval = KubernetesConfig.getConnectionRetryIntervalMs(config);
    }

    public String getKubernetesMasterHost() {
        return kubernetesClient.getMasterUrl().getHost();
    }

    public ConfigMap createOrReplaceConfigMap(ConfigMap configMap) {
        return runWithRetries(() -> kubernetesClient.configMaps().createOrReplace(configMap));
    }

    public Service getService(String serviceName) {
        return runWithRetries((() -> kubernetesClient.services().withName(serviceName).get()));
    }

    public PodList getPods(Map<String, String> labels) {
        return runWithRetries((() -> kubernetesClient.pods().withLabels(labels).list()));
    }

    public ConfigMap getConfigMap(String configmapName) {
        return runWithRetries((() -> kubernetesClient.configMaps().withName(configmapName).get()));
    }

    public Deployment getDeployment(String name) {
        return runWithRetries((() -> kubernetesClient.apps().deployments().withName(name).get()));
    }

    public Service createService(Service service) {
        Callable<Service> action = () -> {
            LOGGER.info("create service: {}", service.getMetadata().getName());
            return kubernetesClient.services().create(service);
        };
        return runWithRetries(action);
    }

    public void createPod(Pod pod) {
        Callable<Void> action = () -> {
            LOGGER.info("create pod: {}", pod.getMetadata().getName());
            kubernetesClient.pods().create(pod);
            return null;
        };
        runWithRetries(action);
    }

    public ConfigMap createConfigMap(ConfigMap configMap) {
        Callable<ConfigMap> action = () -> {
            LOGGER.info("create configmap: {}", configMap.getMetadata().getName());
            return kubernetesClient.configMaps().create(configMap);
        };
        return runWithRetries(action);
    }

    public void createDeployment(Deployment deployment) {
        Callable<Void> action = () -> {
            kubernetesClient.apps().deployments().create(deployment);
            LOGGER.info("create deployment: {}", deployment.getMetadata().getName());
            return null;
        };
        runWithRetries(action);
    }

    private <T> T runWithRetries(Callable<T> action) {
        return RetryCommand.run(action, retryCount, retryInterval);
    }

    public void destroyCluster(String clusterId) {
        String serviceName = masterParam.getServiceName(clusterId);
        LOGGER.info("delete cluster with service:{}", serviceName);
        kubernetesClient.services().withName(serviceName).delete();
    }

    public void deleteConfigMap(String configMapName) {
        LOGGER.info("delete configMap:{}", configMapName);
        kubernetesClient.configMaps().withName(configMapName).delete();
    }

    public void deletePod(Map<String, String> labels) {
        Callable<Void> action = () -> {
            FilterWatchListDeletable<Pod, PodList> running =
                kubernetesClient.pods().withField("status.phase", "Running").withLabels(labels);
            if (!running.list().getItems().isEmpty()) {
                LOGGER.info("delete {} running pod with label:{}", running.list().getItems().size(),
                    labels);
                running.delete();
            }
            return null;
        };
        runWithRetries(action);
    }

    public void close() {
        if (kubernetesClient != null) {
            kubernetesClient.close();
        }
    }

}
