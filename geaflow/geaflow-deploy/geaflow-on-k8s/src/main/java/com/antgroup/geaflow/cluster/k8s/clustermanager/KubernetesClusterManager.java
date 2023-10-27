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

import static com.antgroup.geaflow.cluster.k8s.config.K8SConstants.ENV_IS_RECOVER;
import static com.antgroup.geaflow.cluster.k8s.config.KubernetesConfigKeys.NAME_SPACE;
import static com.antgroup.geaflow.cluster.k8s.config.KubernetesConfigKeys.SERVICE_EXPOSED_TYPE;
import static com.antgroup.geaflow.cluster.k8s.config.KubernetesConfigKeys.SERVICE_SUFFIX;
import static com.antgroup.geaflow.cluster.k8s.config.KubernetesConfigKeys.WATCHER_CHECK_INTERVAL;
import static com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys.CLUSTER_ID;
import static com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys.FO_STRATEGY;

import com.antgroup.geaflow.cluster.clustermanager.AbstractClusterManager;
import com.antgroup.geaflow.cluster.clustermanager.ClusterContext;
import com.antgroup.geaflow.cluster.config.ClusterConfig;
import com.antgroup.geaflow.cluster.failover.FailoverStrategyFactory;
import com.antgroup.geaflow.cluster.failover.IFailoverStrategy;
import com.antgroup.geaflow.cluster.k8s.config.AbstractKubernetesParam;
import com.antgroup.geaflow.cluster.k8s.config.K8SConstants;
import com.antgroup.geaflow.cluster.k8s.config.KubernetesConfig;
import com.antgroup.geaflow.cluster.k8s.config.KubernetesConfig.DockerNetworkType;
import com.antgroup.geaflow.cluster.k8s.config.KubernetesConfig.ServiceExposedType;
import com.antgroup.geaflow.cluster.k8s.config.KubernetesDriverParam;
import com.antgroup.geaflow.cluster.k8s.config.KubernetesMasterParam;
import com.antgroup.geaflow.cluster.k8s.config.KubernetesWorkerParam;
import com.antgroup.geaflow.cluster.k8s.failover.AbstractKubernetesFailoverStrategy;
import com.antgroup.geaflow.cluster.k8s.handler.IPodEventHandler;
import com.antgroup.geaflow.cluster.k8s.handler.PodEvictHandler;
import com.antgroup.geaflow.cluster.k8s.handler.PodOOMHandler;
import com.antgroup.geaflow.cluster.k8s.utils.KubernetesUtils;
import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.exception.GeaflowRuntimeException;
import com.antgroup.geaflow.common.utils.ThreadUtil;
import com.antgroup.geaflow.env.IEnvironment.EnvType;
import com.google.common.annotations.VisibleForTesting;
import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.client.Watch;
import io.fabric8.kubernetes.client.Watcher;
import io.fabric8.kubernetes.client.Watcher.Action;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KubernetesClusterManager extends AbstractClusterManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(KubernetesClusterManager.class);

    private String clusterId;
    private OwnerReference ownerReference;
    private ConfigMap workerConfigMap;

    private KubernetesWorkerParam workerParam;
    private KubernetesDriverParam driverParam;
    private KubernetesMasterParam masterParam;

    private String workerStartCommand;
    private String workerPodNamePrefix;
    private String driverPodNamePrefix;
    private String driverStartCommand;

    private DockerNetworkType dockerNetworkType;
    private ServiceExposedType serviceExposedType;
    private GeaflowKubeClient kubernetesClient;
    private Configuration config;

    private Watch watcher;
    private int checkInterval;
    private volatile boolean watcherClosed;
    private List<IPodEventHandler> eventHandlers;

    @Override
    public void init(ClusterContext context) {
        init(context, new GeaflowKubeClient(context.getConfig()));
    }

    public void init(ClusterContext context, GeaflowKubeClient kubernetesClient) {
        super.init(context);
        this.config = context.getConfig();
        this.kubernetesClient = kubernetesClient;
        this.dockerNetworkType = KubernetesConfig.getDockerNetworkType(config);
        this.serviceExposedType = KubernetesConfig.getServiceExposedType(config);
        this.masterParam = new KubernetesMasterParam(clusterConfig);
        this.clusterId = config.getString(CLUSTER_ID);
        this.checkInterval = config.getInteger(WATCHER_CHECK_INTERVAL);
        this.watcherClosed = true;

        if (config.contains(KubernetesConfig.CLUSTER_START_TIME)) {
            this.workerParam = new KubernetesWorkerParam(clusterConfig);
            this.driverParam = new KubernetesDriverParam(clusterConfig);
            this.workerPodNamePrefix = workerParam.getPodNamePrefix(clusterId);
            this.driverPodNamePrefix = driverParam.getPodNamePrefix(clusterId);
            this.workerStartCommand = workerParam.getContainerShellCommand();
            this.driverStartCommand = driverParam.getContainerShellCommand();
            setupOwnerReference();
            setupConfigMap();
            createPodEventHandlers();
            createAndStartPodsWatcher();
        }
    }

    @Override
    protected IFailoverStrategy buildFailoverStrategy() {
        IFailoverStrategy foStrategy = FailoverStrategyFactory.loadFailoverStrategy(EnvType.K8S,
            super.clusterConfig.getConfig().getString(FO_STRATEGY));
        foStrategy.init(clusterContext);
        ((AbstractKubernetesFailoverStrategy) foStrategy).setClusterManager(this);
        return foStrategy;
    }

    @Override
    public KubernetesClusterId startMaster() {
        config.put(KubernetesConfig.CLUSTER_START_TIME, String.valueOf(System.currentTimeMillis()));
        Map<String, String> labels = masterParam.getPodLabels(clusterId);
        createMaster(clusterId, labels);
        clusterInfo =  new KubernetesClusterId(clusterId);
        return (KubernetesClusterId) clusterInfo;
    }

    private void setupOwnerReference() {
        try {
            String serviceName = KubernetesUtils.getMasterServiceName(clusterId);
            Service service = kubernetesClient.getService(serviceName);
            if (service != null) {
                ownerReference = KubernetesResourceBuilder.createOwnerReference(service);
            } else {
                throw new RuntimeException("Failed to get service: " + serviceName);
            }
        } catch (Exception e) {
            throw new RuntimeException("Could not setup owner reference.", e);
        }
    }

    private void setupConfigMap() {
        try {
            ConfigMap configMap = KubernetesResourceBuilder.createConfigMap(clusterId, workerParam, ownerReference);
            kubernetesClient.createOrReplaceConfigMap(configMap);
            workerConfigMap = configMap;
        } catch (Exception e) {
            throw new RuntimeException("Could not upload worker config map.", e);
        }
    }

    @Override
    public void doStartContainer(int containerId, boolean isRecover) {
        try {
            // Create container.
            String podName = workerPodNamePrefix + containerId;
            Map<String, String> additionalEnvs = workerParam.getAdditionEnvs();
            additionalEnvs.put(ENV_IS_RECOVER, String.valueOf(isRecover));
            Container container = KubernetesResourceBuilder
                .createContainer(podName, String.valueOf(containerId), masterId, workerParam,
                    workerStartCommand, additionalEnvs, dockerNetworkType);

            // Create pod.
            Pod workerPod = KubernetesResourceBuilder
                .createPod(clusterId, podName, String.valueOf(containerId), ownerReference,
                    workerConfigMap, workerParam, container);
            kubernetesClient.createPod(workerPod);
        } catch (Exception e) {
            LOGGER.error("Failed to request new worker node:{}", e.getMessage(), e);
            throw new GeaflowRuntimeException(e);
        }
    }

    @Override
    public void doStartDriver(int driverId, int driverIndex) {
        String serviceName = KubernetesUtils.getDriverServiceName(clusterId, driverIndex);
        Service service = kubernetesClient.getService(serviceName);
        if (service != null) {
            LOGGER.info("driver service {} already exists, skip starting driver", serviceName);
            return;
        }

        // 1. Create container.
        String podName  = driverPodNamePrefix + driverId;
        Map<String, String> additionalEnvs = driverParam.getAdditionEnvs();
        additionalEnvs.put(K8SConstants.ENV_CONTAINER_INDEX, String.valueOf(driverIndex));
        Container container = KubernetesResourceBuilder
            .createContainer(podName, String.valueOf(driverId), masterId, driverParam,
                driverStartCommand, additionalEnvs, dockerNetworkType);

        // 2. Create deployment.
        String rcName = clusterId + K8SConstants.DRIVER_RS_NAME_SUFFIX + driverIndex;
        Deployment deployment = KubernetesResourceBuilder
            .createDeployment(clusterId, rcName, container, workerConfigMap,
                driverParam, dockerNetworkType);

        // 3. Create the service.
        createService(serviceName, serviceExposedType,
            driverParam.getPodLabels(clusterId), ownerReference, driverParam);

        // 4. Set owner reference.
        deployment.getMetadata().setOwnerReferences(Collections.singletonList(ownerReference));

        kubernetesClient.createDeployment(deployment);
    }

    @VisibleForTesting
    public void createMaster(String clusterId, Map<String, String> labels) {
        this.clusterId = clusterId;
        this.config.put(CLUSTER_ID, clusterId);
        this.dockerNetworkType = KubernetesConfig.getDockerNetworkType(config);
        // Host network only supports clusterIp
        if (dockerNetworkType == DockerNetworkType.HOST) {
            config.put(SERVICE_EXPOSED_TYPE, ServiceExposedType.CLUSTER_IP.name());
        }
        this.serviceExposedType = KubernetesConfig.getServiceExposedType(config);

        // 1. create configMap.
        ConfigMap configMap = createMasterConfigMap(clusterId, dockerNetworkType);

        // 2. create the master container
        Container container = createMasterContainer(clusterId, dockerNetworkType);

        // 3. create replication controller.
        String masterDeployName = clusterId + K8SConstants.MASTER_RS_NAME_SUFFIX;
        Deployment deployment = KubernetesResourceBuilder
            .createDeployment(clusterId, masterDeployName, container, configMap, masterParam,
                dockerNetworkType);

        // 3. create the service.
        String serviceName = KubernetesUtils.getMasterServiceName(clusterId);
        Service service = createService(serviceName,
            ServiceExposedType.CLUSTER_IP, labels, null, masterParam);
        OwnerReference ownerReference = KubernetesResourceBuilder.createOwnerReference(service);

        if (!serviceExposedType.equals(ServiceExposedType.CLUSTER_IP)) {
            serviceName = KubernetesUtils.getMasterClientServiceName(clusterId);
            createService(serviceName, ServiceExposedType.NODE_PORT, labels, ownerReference,
                masterParam);
        }

        // 4. set owner reference.
        deployment.getMetadata().setOwnerReferences(Collections.singletonList(ownerReference));
        configMap.getMetadata().setOwnerReferences(Collections.singletonList(ownerReference));

        kubernetesClient.createConfigMap(configMap);
        kubernetesClient.createDeployment(deployment);
    }

    @VisibleForTesting
    public Container createMasterContainer(String clusterId, DockerNetworkType networkType) {
        String containerName = masterParam.getContainerName();
        String containerId = clusterId + K8SConstants.MASTER_NAME_SUFFIX;
        String command = masterParam.getContainerShellCommand();
        LOGGER.info("master start command: {}", command);

        Map<String, String> additionalEnvs = masterParam.getAdditionEnvs();
        return KubernetesResourceBuilder
            .createContainer(containerName, containerId, masterId, masterParam, command,
                additionalEnvs, networkType);
    }

    /**
     * Setup a Config Map that will generate a geaflow-conf.yaml and log4j file.
     * @param clusterId the cluster id
     * @return the created configMap
     */
    public ConfigMap createMasterConfigMap(String clusterId, DockerNetworkType dockerNetworkType) {
        if (dockerNetworkType != DockerNetworkType.HOST) {
            // use serviceName to discover master
            String namespace = config.getString(NAME_SPACE);
            String serviceSuffix = config.getString(SERVICE_SUFFIX);
            serviceSuffix = StringUtils.isBlank(serviceSuffix) ? "" : K8SConstants.NAMESPACE_SEPARATOR
                + serviceSuffix;
            config.put(ClusterConfig.MASTER_ADDRESS,
                clusterId + K8SConstants.SERVICE_NAME_SUFFIX + K8SConstants.NAMESPACE_SEPARATOR
                    + namespace + serviceSuffix);
        }

        return KubernetesResourceBuilder.createConfigMap(clusterId, masterParam, null);
    }

    private Service createService(String serviceName, ServiceExposedType exposedType,
                                  Map<String, String> labels, OwnerReference ownerReference,
                                  AbstractKubernetesParam param) {
        Service service = KubernetesResourceBuilder
            .createService(serviceName, exposedType, labels, ownerReference, param);
        return kubernetesClient.createService(service);
    }

    public void restartDriver(int driverId) {
        LOGGER.info("Kill driver pod: {}.", driverId);
        Map<String, String> labels = new HashMap<>();
        labels.put(K8SConstants.LABEL_APP_KEY, clusterId);
        labels.put(K8SConstants.LABEL_COMPONENT_KEY, K8SConstants.LABEL_COMPONENT_DRIVER);
        labels.put(K8SConstants.LABEL_COMPONENT_ID_KEY, String.valueOf(driverId));
        kubernetesClient.deletePod(labels);
    }

    @Override
    public void restartContainer(int containerId) {
        // Kill the pod before start a new one.
        Map<String, String> labels = new HashMap<>();
        labels.put(K8SConstants.LABEL_APP_KEY, clusterId);
        labels.put(K8SConstants.LABEL_COMPONENT_KEY, K8SConstants.LABEL_COMPONENT_WORKER);
        labels.put(K8SConstants.LABEL_COMPONENT_ID_KEY, String.valueOf(containerId));
        kubernetesClient.deletePod(labels);
        doStartContainer(containerId, true);
    }

    /**
     * Kill all driver processes and the killed process will be restarted by k8s.
     */
    public void restartAllDrivers() {
        Map<String, String> labels = new HashMap<>();
        labels.put(K8SConstants.LABEL_APP_KEY, clusterId);
        labels.put(K8SConstants.LABEL_COMPONENT_KEY, K8SConstants.LABEL_COMPONENT_DRIVER);
        kubernetesClient.deletePod(labels);
    }

    public void restartAllContainers() {
        Map<Integer, String> containerIds = clusterContext.getContainerIds();
        LOGGER.info("Restart {} containers caused by failover.", containerIds.keySet().size());
        Map<String, String> labels = new HashMap<>();
        labels.put(K8SConstants.LABEL_APP_KEY, clusterId);
        labels.put(K8SConstants.LABEL_COMPONENT_KEY, K8SConstants.LABEL_COMPONENT_WORKER);
        kubernetesClient.deletePod(labels);
        for (Entry<Integer, String> entry : containerIds.entrySet()) {
            doStartContainer(entry.getKey(), true);
        }
    }

    /**
     * Kill all container processes.
     */
    public void killAllContainers() {
        Map<String, String> labels = new HashMap<>();
        labels.put(K8SConstants.LABEL_APP_KEY, clusterId);
        labels.put(K8SConstants.LABEL_COMPONENT_KEY, K8SConstants.LABEL_COMPONENT_WORKER);
        kubernetesClient.deletePod(labels);
    }

    private void createPodEventHandlers() {
        eventHandlers = new ArrayList<>();
        eventHandlers.add(new PodOOMHandler());
        eventHandlers.add(new PodEvictHandler(config));
    }

    private void createAndStartPodsWatcher() {
        BiConsumer<Action, Pod> eventHandler = this::handlePodMessage;
        Consumer<Exception> exceptionHandler = (exception) -> {
            watcherClosed = true;
            LOGGER.warn("watch exception: {}", exception.getMessage(), exception);
        };

        ScheduledExecutorService executorService =
            Executors.newSingleThreadScheduledExecutor(ThreadUtil.namedThreadFactory(true, "watcher-creator"));
        executorService.scheduleAtFixedRate(() -> {
            if (watcherClosed) {
                if (watcher != null) {
                    watcher.close();
                }
                watcher = kubernetesClient.createPodsWatcher(workerParam.getPodLabels(clusterId),
                    eventHandler, exceptionHandler);
                if (watcher != null) {
                    watcherClosed = false;
                }
            }
        }, checkInterval, checkInterval, TimeUnit.SECONDS);
    }

    protected void handlePodMessage(Watcher.Action action, Pod pod) {
        String componentId = KubernetesUtils.extractComponentId(pod);
        if (componentId == null) {
            LOGGER.warn("Unexpected pod {} event:{}", pod.getMetadata().getName(), action);
            return;
        }
        if (action == Action.MODIFIED) {
            eventHandlers.forEach(h -> h.handle(pod));
        } else {
            LOGGER.info("Skip handling {} event for pod {}", action, pod.getMetadata().getName());
        }
    }

    @Override
    public void close() {
        super.close();
        if (kubernetesClient != null) {
            kubernetesClient.destroyCluster(clusterId);
            kubernetesClient.close();
        }
    }
}
