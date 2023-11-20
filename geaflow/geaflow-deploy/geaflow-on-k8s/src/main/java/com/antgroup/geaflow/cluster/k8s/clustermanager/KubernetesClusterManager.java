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

import static com.antgroup.geaflow.cluster.constants.ClusterConstants.DEFAULT_MASTER_ID;
import static com.antgroup.geaflow.cluster.k8s.config.K8SConstants.CONTAINER_START_COMMAND;
import static com.antgroup.geaflow.cluster.k8s.config.K8SConstants.ENV_IS_RECOVER;
import static com.antgroup.geaflow.cluster.k8s.config.KubernetesConfigKeys.LOG_DIR;
import static com.antgroup.geaflow.cluster.k8s.config.KubernetesConfigKeys.NAME_SPACE;
import static com.antgroup.geaflow.cluster.k8s.config.KubernetesConfigKeys.SERVICE_EXPOSED_TYPE;
import static com.antgroup.geaflow.cluster.k8s.config.KubernetesConfigKeys.SERVICE_SUFFIX;
import static com.antgroup.geaflow.cluster.k8s.config.KubernetesConfigKeys.WATCHER_CHECK_INTERVAL;
import static com.antgroup.geaflow.cluster.k8s.config.KubernetesContainerParam.CONTAINER_LOG_SUFFIX;
import static com.antgroup.geaflow.cluster.k8s.config.KubernetesDriverParam.DRIVER_LOG_SUFFIX;
import static com.antgroup.geaflow.cluster.k8s.config.KubernetesMasterParam.MASTER_LOG_SUFFIX;
import static com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys.CLUSTER_ID;
import static com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys.FO_STRATEGY;

import com.antgroup.geaflow.cluster.clustermanager.AbstractClusterManager;
import com.antgroup.geaflow.cluster.clustermanager.ClusterContext;
import com.antgroup.geaflow.cluster.constants.ClusterConstants;
import com.antgroup.geaflow.cluster.failover.FailoverStrategyFactory;
import com.antgroup.geaflow.cluster.failover.IFailoverStrategy;
import com.antgroup.geaflow.cluster.k8s.config.AbstractKubernetesParam;
import com.antgroup.geaflow.cluster.k8s.config.K8SConstants;
import com.antgroup.geaflow.cluster.k8s.config.KubernetesConfig;
import com.antgroup.geaflow.cluster.k8s.config.KubernetesConfig.DockerNetworkType;
import com.antgroup.geaflow.cluster.k8s.config.KubernetesConfig.ServiceExposedType;
import com.antgroup.geaflow.cluster.k8s.config.KubernetesContainerParam;
import com.antgroup.geaflow.cluster.k8s.config.KubernetesDriverParam;
import com.antgroup.geaflow.cluster.k8s.config.KubernetesMasterParam;
import com.antgroup.geaflow.cluster.k8s.entrypoint.KubernetesSupervisorRunner;
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
import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
    private ConfigMap containerConfigMap;

    private KubernetesContainerParam containerParam;
    private KubernetesDriverParam driverParam;
    private KubernetesMasterParam masterParam;

    private String containerStartCommand;
    private String containerPodNamePrefix;
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
            this.containerParam = new KubernetesContainerParam(clusterConfig);
            this.driverParam = new KubernetesDriverParam(clusterConfig);
            this.containerPodNamePrefix = containerParam.getPodNamePrefix(clusterId);
            this.containerStartCommand = containerParam.getContainerShellCommand();
            this.driverPodNamePrefix = driverParam.getPodNamePrefix(clusterId);
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
            ConfigMap configMap = KubernetesResourceBuilder.createConfigMap(clusterId,
                containerParam, ownerReference);
            kubernetesClient.createOrReplaceConfigMap(configMap);
            containerConfigMap = configMap;
        } catch (Exception e) {
            throw new RuntimeException("Could not upload container config map.", e);
        }
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
            .createDeployment(clusterId, masterDeployName, String.valueOf(DEFAULT_MASTER_ID),
                container, configMap, masterParam, dockerNetworkType);

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
        additionalEnvs.put(CONTAINER_START_COMMAND, command);

        String startCommand = buildSupervisorStartCommand(MASTER_LOG_SUFFIX);
        return KubernetesResourceBuilder
            .createContainer(containerName, containerId, masterId, masterParam, startCommand,
                additionalEnvs, networkType);
    }

    /**
     * Set up a Config Map that will generate a geaflow-conf.yaml and log4j file.
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
            config.put(ClusterConstants.MASTER_ADDRESS,
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


    @Override
    public void createNewContainer(int containerId, boolean isRecover) {
        try {
            // Create container.
            String podName = containerPodNamePrefix + containerId;
            Map<String, String> additionalEnvs = containerParam.getAdditionEnvs();
            additionalEnvs.put(ENV_IS_RECOVER, String.valueOf(isRecover));
            additionalEnvs.put(CONTAINER_START_COMMAND, containerStartCommand);

            String startCommand = buildSupervisorStartCommand(CONTAINER_LOG_SUFFIX);
            Container container = KubernetesResourceBuilder
                .createContainer(podName, String.valueOf(containerId), masterId, containerParam,
                    startCommand, additionalEnvs, dockerNetworkType);

            // Create pod.
            Pod containerPod = KubernetesResourceBuilder
                .createPod(clusterId, podName, String.valueOf(containerId), ownerReference,
                    containerConfigMap, containerParam, container);
            kubernetesClient.createPod(containerPod);
        } catch (Exception e) {
            LOGGER.error("Failed to request new container pod:{}", e.getMessage(), e);
            throw new GeaflowRuntimeException(e);
        }
    }

    @Override
    public void createNewDriver(int driverId, int driverIndex) {
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
        additionalEnvs.put(CONTAINER_START_COMMAND, driverStartCommand);

        String startCommand = buildSupervisorStartCommand(DRIVER_LOG_SUFFIX);
        Container container = KubernetesResourceBuilder
            .createContainer(podName, String.valueOf(driverId), masterId, driverParam,
                startCommand, additionalEnvs, dockerNetworkType);

        // 2. Create deployment.
        String rcName = clusterId + K8SConstants.DRIVER_RS_NAME_SUFFIX + driverIndex;
        Deployment deployment = KubernetesResourceBuilder
            .createDeployment(clusterId, rcName, String.valueOf(driverId), container,
                containerConfigMap, driverParam, dockerNetworkType);

        // 3. Create the service.
        createService(serviceName, serviceExposedType,
            driverParam.getPodLabels(clusterId), ownerReference, driverParam);

        // 4. Set owner reference.
        deployment.getMetadata().setOwnerReferences(Collections.singletonList(ownerReference));

        kubernetesClient.createDeployment(deployment);
    }

    @Override
    public void recreateDriver(int driverId) {
        LOGGER.info("Kill driver pod: {}.", driverId);
        Map<String, String> labels = new HashMap<>();
        labels.put(K8SConstants.LABEL_APP_KEY, clusterId);
        labels.put(K8SConstants.LABEL_COMPONENT_KEY, K8SConstants.LABEL_COMPONENT_DRIVER);
        labels.put(K8SConstants.LABEL_COMPONENT_ID_KEY, String.valueOf(driverId));
        kubernetesClient.deletePod(labels);
    }

    @Override
    public void recreateContainer(int containerId) {
        // Kill the pod before start a new one.
        Map<String, String> labels = new HashMap<>();
        labels.put(K8SConstants.LABEL_APP_KEY, clusterId);
        labels.put(K8SConstants.LABEL_COMPONENT_KEY, K8SConstants.LABEL_COMPONENT_WORKER);
        labels.put(K8SConstants.LABEL_COMPONENT_ID_KEY, String.valueOf(containerId));
        kubernetesClient.deletePod(labels);
        createNewContainer(containerId, true);
    }

    private String buildSupervisorStartCommand(String fileName) {
        String logFile = config.getString(LOG_DIR) + File.separator + fileName;
        return KubernetesUtils.getContainerStartCommand(clusterConfig.getSupervisorJvmOptions(),
            KubernetesSupervisorRunner.class, logFile, config);
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
                watcher = kubernetesClient.createPodsWatcher(containerParam.getPodLabels(clusterId),
                    eventHandler, exceptionHandler);
                if (watcher != null) {
                    watcherClosed = false;
                }
            }
        }, checkInterval, checkInterval, TimeUnit.SECONDS);
    }

    private void handlePodMessage(Watcher.Action action, Pod pod) {
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
