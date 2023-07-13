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

import static com.antgroup.geaflow.cluster.k8s.config.KubernetesConfigKeys.NAME_SPACE;
import static com.antgroup.geaflow.cluster.k8s.config.KubernetesConfigKeys.SERVICE_EXPOSED_TYPE;
import static com.antgroup.geaflow.cluster.k8s.config.KubernetesConfigKeys.SERVICE_SUFFIX;
import static com.antgroup.geaflow.cluster.k8s.utils.K8SConstants.CLIENT_SERVICE_NAME_SUFFIX;
import static com.antgroup.geaflow.cluster.k8s.utils.K8SConstants.ENV_IS_RECOVER;
import static com.antgroup.geaflow.cluster.k8s.utils.K8SConstants.LABEL_COMPONENT_DRIVER;
import static com.antgroup.geaflow.cluster.k8s.utils.K8SConstants.LABEL_COMPONENT_MASTER;
import static com.antgroup.geaflow.cluster.k8s.utils.K8SConstants.LABEL_COMPONENT_WORKER;
import static com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys.CLUSTER_ID;
import static com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys.FO_STRATEGY;

import com.antgroup.geaflow.cluster.clustermanager.AbstractClusterManager;
import com.antgroup.geaflow.cluster.clustermanager.ClusterContext;
import com.antgroup.geaflow.cluster.config.ClusterConfig;
import com.antgroup.geaflow.cluster.container.ContainerInfo;
import com.antgroup.geaflow.cluster.driver.DriverInfo;
import com.antgroup.geaflow.cluster.failover.FailoverStrategyFactory;
import com.antgroup.geaflow.cluster.failover.IFailoverStrategy;
import com.antgroup.geaflow.cluster.k8s.config.AbstractKubernetesParam;
import com.antgroup.geaflow.cluster.k8s.config.KubernetesConfig;
import com.antgroup.geaflow.cluster.k8s.config.KubernetesConfig.DockerNetworkType;
import com.antgroup.geaflow.cluster.k8s.config.KubernetesConfig.ServiceExposedType;
import com.antgroup.geaflow.cluster.k8s.config.KubernetesDriverParam;
import com.antgroup.geaflow.cluster.k8s.config.KubernetesMasterParam;
import com.antgroup.geaflow.cluster.k8s.config.KubernetesWorkerParam;
import com.antgroup.geaflow.cluster.k8s.failover.AbstractKubernetesFailoverStrategy;
import com.antgroup.geaflow.cluster.k8s.utils.K8SConstants;
import com.antgroup.geaflow.cluster.rpc.RpcAddress;
import com.antgroup.geaflow.cluster.rpc.RpcUtil;
import com.antgroup.geaflow.cluster.system.ClusterMetaStore;
import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.exception.GeaflowRuntimeException;
import com.antgroup.geaflow.env.IEnvironment.EnvType;
import com.antgroup.geaflow.rpc.proto.Master.RegisterResponse;
import com.google.common.annotations.VisibleForTesting;
import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
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

    private static final String LABEL_DRIVER_INDEX_PATTERN = "driver-index-%s";

    @Override
    public void init(ClusterContext context) {
        init(context, new GeaflowKubeClient(context.getConfig()));
    }

    public void init(ClusterContext context, GeaflowKubeClient kubernetesClient) {
        super.init(context);
        Configuration config = context.getConfig();
        this.kubernetesClient = kubernetesClient;
        this.dockerNetworkType = KubernetesConfig.getDockerNetworkType(config);
        this.serviceExposedType = KubernetesConfig.getServiceExposedType(config);
        this.masterParam = new KubernetesMasterParam(clusterConfig);
        this.clusterId = config.getString(CLUSTER_ID);
        this.config = config;

        if (config.contains(KubernetesConfig.CLUSTER_START_TIME)) {
            this.workerParam = new KubernetesWorkerParam(clusterConfig);
            this.driverParam = new KubernetesDriverParam(clusterConfig);
            this.workerPodNamePrefix = workerParam.getPodNamePrefix(clusterId);
            this.driverPodNamePrefix = driverParam.getPodNamePrefix(clusterId);
            this.workerStartCommand = workerParam.getContainerShellCommand();
            this.driverStartCommand = driverParam.getContainerShellCommand();
            setupOwnerReference();
            setupConfigMap();
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
            String serviceName = workerParam.getServiceName(clusterId);
            Service service = kubernetesClient.getService(serviceName);
            if (service != null) {
                ownerReference = KubernetesResourceBuilder.createOwnerReference(service);
            } else {
                throw new RuntimeException("Failed to get service " + clusterId + K8SConstants.SERVICE_NAME_SUFFIX);
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

    public RegisterResponse registerContainer(ContainerInfo request) {
        LOGGER.info("received container registration. {}", request);
        ContainerInfo containerInfo = containerInfos.put(request.getId(), request);
        boolean registered = (containerInfo == null);
        RegisterResponse response = RegisterResponse.newBuilder().setSuccess(registered).build();
        RpcUtil.asyncExecute(() -> openContainer(request));
        LOGGER.info("container registration success. {}", request);
        return response;
    }

    public RegisterResponse registerDriver(DriverInfo driverInfo) {
        LOGGER.info("received driver registration. {}", driverInfo);
        driverInfos.put(driverInfo.getId(), driverInfo);
        boolean registered = driverFuture.complete(driverInfo);
        LOGGER.info("driver is registered:{}", driverInfo);
        return RegisterResponse.newBuilder().setSuccess(registered).build();
    }

    @Override
    public void restartContainer(int containerId) {
        // kill the pod before start a new one.
        Map<String, String> labels = new HashMap<>();
        labels.put(K8SConstants.LABEL_APP_KEY, clusterId);
        labels.put(K8SConstants.LABEL_COMPONENT_KEY, K8SConstants.LABEL_COMPONENT_WORKER);
        labels.put(K8SConstants.LABEL_COMPONENT_ID_KEY, String.valueOf(containerId));
        kubernetesClient.deletePod(labels);
        doStartContainer(containerId, true);
    }

    public void killMaster() {
        LOGGER.info("Kill master pod caused by Failover.");
        Map<String, String> labels = new HashMap<>();
        labels.put(K8SConstants.LABEL_APP_KEY, clusterId);
        labels.put(K8SConstants.LABEL_COMPONENT_KEY, LABEL_COMPONENT_MASTER);
        kubernetesClient.deletePod(labels);
    }

    public void killDriver() {
        // Kill the driver pod before start a new one.
        LOGGER.info("Kill driver pod caused by Failover.");
        Map<String, String> labels = new HashMap<>();
        labels.put(K8SConstants.LABEL_APP_KEY, clusterId);
        labels.put(K8SConstants.LABEL_COMPONENT_KEY, LABEL_COMPONENT_DRIVER);
        kubernetesClient.deletePod(labels);
    }

    public void killAllContainers() {
        LOGGER.info("Kill all container pods caused by Failover.");
        Map<String, String> labels = new HashMap<>();
        labels.put(K8SConstants.LABEL_APP_KEY, clusterId);
        labels.put(K8SConstants.LABEL_COMPONENT_KEY, LABEL_COMPONENT_WORKER);
        kubernetesClient.deletePod(labels);
    }

    public void startAllContainersByFailover() {
        for (Integer index : this.containerIds) {
            LOGGER.info("Restart container {} caused by Failover.", index);
            doStartContainer(index, true);
        }
    }

    @Override
    public void doStartContainer(int containerId, boolean isRecover) {
        try {
            // create container
            String podName = workerPodNamePrefix + containerId;
            Map<String, String> additionalEnvs = workerParam.getAdditionEnvs();
            additionalEnvs.put(ENV_IS_RECOVER, String.valueOf(isRecover));
            Container container = KubernetesResourceBuilder
                .createContainer(podName, String.valueOf(containerId), masterId, workerParam,
                    workerStartCommand, additionalEnvs, dockerNetworkType);

            // create pod
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
    public void doStartDriver(int driverId) {

        String serviceName = driverParam.getServiceName(clusterId);
        Service service = kubernetesClient.getService(serviceName);
        if (service != null) {
            LOGGER.info("driver service {} already exists, skip starting driver", serviceName);
            return;
        }

        // create container
        String podName  = driverPodNamePrefix + driverId;
        Map<String, String> additionalEnvs = driverParam.getAdditionEnvs();
        Container container = KubernetesResourceBuilder
            .createContainer(podName, String.valueOf(driverId), masterId, driverParam,
                driverStartCommand, additionalEnvs, dockerNetworkType);

        // 3. create replication controller.
        String rcName = clusterId + K8SConstants.DRIVER_RS_NAME_SUFFIX;
        Deployment deployment = KubernetesResourceBuilder
            .createDeployment(clusterId, rcName, container, workerConfigMap,
                driverParam, dockerNetworkType);

        // 3. create the service.
        createService(driverParam.getServiceName(clusterId), ServiceExposedType.CLUSTER_IP,
            driverParam.getPodLabels(clusterId), ownerReference, driverParam);

        // 4. set owner reference.
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
        Service service = createService(masterParam.getServiceName(clusterId),
            ServiceExposedType.CLUSTER_IP, labels, null, masterParam);
        OwnerReference ownerReference = KubernetesResourceBuilder.createOwnerReference(service);

        if (!serviceExposedType.equals(ServiceExposedType.CLUSTER_IP)) {
            String serviceName = clusterId + CLIENT_SERVICE_NAME_SUFFIX;
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

        return KubernetesResourceBuilder
            .createContainer(containerName, containerId, masterId, masterParam, command,
                masterParam.getAdditionEnvs(), networkType);
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

    @Override
    public RpcAddress startDriver() {

        String driverIndexLabel = String.format(LABEL_DRIVER_INDEX_PATTERN, clusterId);
        Set<Integer> driverIndex = ClusterMetaStore.getInstance().getComponentIds(driverIndexLabel);
        LOGGER.info("Get driver index {} of label {} from backend.", driverIndex, driverIndexLabel);
        boolean isRecover = driverIndex != null;
        RpcAddress rpcAddress;
        if (isRecover) {
            try {
                super.setDriverIds(driverIndex);
                DriverInfo driverInfo = driverFuture.get(driverTimeoutSec, TimeUnit.SECONDS);
                rpcAddress = new RpcAddress(driverInfo.getHost(), driverInfo.getRpcPort());
            } catch (InterruptedException | TimeoutException | ExecutionException e) {
                throw new GeaflowRuntimeException(e);
            }
        } else {
            rpcAddress = super.startDriver();
            driverIndex = super.getDriverIds();
            ClusterMetaStore.getInstance().saveComponentIndex(driverIndexLabel, driverIndex);
            LOGGER.info("Saved driver index {} of label {} to backend.", driverIndex, driverIndexLabel);
        }
        return rpcAddress;
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
