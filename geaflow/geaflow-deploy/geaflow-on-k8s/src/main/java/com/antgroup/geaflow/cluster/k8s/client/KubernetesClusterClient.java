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

import static com.antgroup.geaflow.cluster.constants.ClusterConstants.PORT_SEPARATOR;
import static com.antgroup.geaflow.cluster.k8s.config.KubernetesConfig.DRIVER_EXPOSED_ADDRESS;
import static com.antgroup.geaflow.cluster.k8s.config.KubernetesConfig.MASTER_EXPOSED_ADDRESS;
import static com.antgroup.geaflow.cluster.k8s.config.KubernetesConfig.ServiceExposedType.CLUSTER_IP;
import static com.antgroup.geaflow.cluster.k8s.config.KubernetesConfigKeys.NAME_SPACE;
import static com.antgroup.geaflow.cluster.k8s.config.KubernetesConfigKeys.SERVICE_SUFFIX;
import static com.antgroup.geaflow.cluster.k8s.config.KubernetesDriverParam.DRIVER_SERVICE_NAME_SUFFIX;
import static com.antgroup.geaflow.cluster.k8s.utils.K8SConstants.CLIENT_SERVICE_NAME_SUFFIX;
import static com.antgroup.geaflow.cluster.k8s.utils.K8SConstants.SERVICE_NAME_SUFFIX;
import static com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys.JOB_WORK_PATH;

import com.antgroup.geaflow.cluster.client.AbstractClusterClient;
import com.antgroup.geaflow.cluster.client.IPipelineClient;
import com.antgroup.geaflow.cluster.client.PipelineClient;
import com.antgroup.geaflow.cluster.client.callback.ClusterStartedCallback.ClusterMeta;
import com.antgroup.geaflow.cluster.clustermanager.ClusterContext;
import com.antgroup.geaflow.cluster.k8s.clustermanager.GeaflowKubeClient;
import com.antgroup.geaflow.cluster.k8s.clustermanager.KubernetesClusterManager;
import com.antgroup.geaflow.cluster.k8s.config.KubernetesConfig;
import com.antgroup.geaflow.cluster.k8s.config.KubernetesConfig.DockerNetworkType;
import com.antgroup.geaflow.cluster.k8s.config.KubernetesConfig.ServiceExposedType;
import com.antgroup.geaflow.cluster.k8s.config.KubernetesMasterParam;
import com.antgroup.geaflow.cluster.k8s.utils.K8SConstants;
import com.antgroup.geaflow.cluster.k8s.utils.KubernetesUtils;
import com.antgroup.geaflow.cluster.rpc.RpcAddress;
import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.exception.GeaflowRuntimeException;
import com.antgroup.geaflow.common.utils.RetryCommand;
import com.antgroup.geaflow.common.utils.SleepUtils;
import com.antgroup.geaflow.env.ctx.IEnvironmentContext;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.LoadBalancerIngress;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServicePort;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KubernetesClusterClient extends AbstractClusterClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(KubernetesClusterClient.class);
    private static final long DEFAULT_SLEEP_MS = 1000;

    private GeaflowKubeClient kubernetesClient;
    private KubernetesMasterParam masterParam;

    private int clientTimeoutMs;
    private KubernetesClusterManager clusterManager;

    @Override
    public void init(IEnvironmentContext environmentContext) {
        super.init(environmentContext);
        this.config.put(JOB_WORK_PATH, KubernetesConfig.getJobWorkDir(config, clusterId));
        this.masterParam = new KubernetesMasterParam(config);
        this.clusterManager = new KubernetesClusterManager();
        String masterUrl = KubernetesConfig.getClientMasterUrl(config);
        this.kubernetesClient = new GeaflowKubeClient(config, masterUrl);
        this.clusterManager.init(new ClusterContext(config), kubernetesClient);
        this.clientTimeoutMs = KubernetesConfig.getClientTimeoutMs(config);
    }

    @Override
    public IPipelineClient startCluster() {
        try {
            this.clusterId = clusterManager.startMaster().getHandler();
            Configuration config = getClientConfig(clusterId);
            LOGGER.info("Cluster info: {}.", config);
            String driverAddress = config.getString(DRIVER_EXPOSED_ADDRESS);
            Preconditions.checkArgument(StringUtils.isNoneEmpty(driverAddress),
                "Driver address is empty.");
            // waiting for driver rpc connection ready
            RpcAddress driverRpcAddress = RpcAddress.build(driverAddress);
            checkRpcConnection(driverRpcAddress);
            ClusterMeta clusterMeta = new ClusterMeta(driverAddress,
                config.getString(MASTER_EXPOSED_ADDRESS));
            callback.onSuccess(clusterMeta);
            return new PipelineClient(driverRpcAddress, config);
        } catch (Throwable e) {
            LOGGER.error("Deploy failed.", e);
            callback.onFailure(e);
            SleepUtils.sleepSecond(5);
            kubernetesClient.destroyCluster(clusterId);
            throw new GeaflowRuntimeException(e);
        }
    }

    @VisibleForTesting
    public void checkRpcConnection(RpcAddress rpcAddress) {
        LOGGER.info("Try checking rpc connection to address {}.", rpcAddress.getAddress());
        RetryCommand.run(() -> {
            checkSocketConnection(rpcAddress.getHost(), rpcAddress.getPort());
            return null;
        }, 5, 5000);
        LOGGER.info("Check rpc connection to address {} success.", rpcAddress.getAddress());
    }

    private void checkSocketConnection(String ip, Integer port)  throws IOException {
        Socket socket = new Socket();
        try {
            socket.connect(new InetSocketAddress(ip, port), 3000);
        } finally {
            try {
                socket.close();
            } catch (IOException e) {
                LOGGER.error("Close socket error.", e);
            }
        }
    }

    private Configuration getClientConfig(String clusterId) throws TimeoutException {
        Configuration config = masterParam.getConfig();
        DockerNetworkType networkType = KubernetesConfig.getDockerNetworkType(config);
        if (networkType == DockerNetworkType.HOST) {
            Map<String, String> masterConfig = waitForConfigMapUpdated(clusterId);
            config.putAll(masterConfig);
        } else {
            Map<String, String> serviceConfig = setupExposedServiceAddress(clusterId);
            config.putAll(serviceConfig);
            waitForConfigMapUpdated(clusterId);
        }
        return config;
    }

    private Map<String, String> waitForConfigMapUpdated(String clusterId) throws TimeoutException {
        Map<String, String> configuration;
        final long startTime = System.currentTimeMillis();
        while (true) {
            ConfigMap configMap = kubernetesClient
                .getConfigMap(masterParam.getConfigMapName(clusterId));
            configuration = KubernetesUtils
                .loadConfigurationFromString(configMap.getData().get(K8SConstants.ENV_CONFIG_FILE));
            if (configuration.containsKey(DRIVER_EXPOSED_ADDRESS)) {
                break;
            }
            long elapsedTime = System.currentTimeMillis() - startTime;
            if (elapsedTime > 60000) {
                LOGGER.warn("Start cluster took more than 60 seconds, please check logs on the "
                    + "Kubernetes cluster.");
                if (elapsedTime > clientTimeoutMs) {
                    throw new TimeoutException("Waiting cluster ready timeout.");
                }
            }
            SleepUtils.sleepMilliSecond(DEFAULT_SLEEP_MS);
        }
        return configuration;
    }

    @Override
    public void shutdown() {
        Preconditions.checkNotNull(clusterId, "ClusterId is null.");
        kubernetesClient.destroyCluster(clusterId);
    }

    private Map<String, String> setupExposedServiceAddress(String clusterId)
        throws TimeoutException {
        ServiceExposedType serviceType = KubernetesConfig.getServiceExposedType(config);
        String masterServiceName;
        if (serviceType == CLUSTER_IP) {
            masterServiceName = clusterId + SERVICE_NAME_SUFFIX;
        } else {
            masterServiceName = clusterId + CLIENT_SERVICE_NAME_SUFFIX;
        }
        String serviceAddress = setupExposedServiceAddress(serviceType, masterServiceName,
            K8SConstants.HTTP_PORT);
        Map<String, String> config = new HashMap<>();
        config.put(MASTER_EXPOSED_ADDRESS, serviceAddress);

        String driverServiceName = clusterId + DRIVER_SERVICE_NAME_SUFFIX;
        serviceAddress = setupExposedServiceAddress(CLUSTER_IP, driverServiceName,
            K8SConstants.RPC_PORT);
        config.put(DRIVER_EXPOSED_ADDRESS, serviceAddress);
        return config;
    }

    private String setupExposedServiceAddress(ServiceExposedType serviceType, String serviceName,
                                              String portName) throws TimeoutException {
        String serviceAddress = "localhost";
        switch (serviceType) {
            case CLUSTER_IP:
                serviceAddress = resolveServiceExposedByClusterIp(serviceName, portName);
                break;
            case NODE_PORT:
                serviceAddress = resolveServiceExposedByNodePort(serviceName, portName);
                break;
            case LOAD_BALANCER:
                serviceAddress = resolveServiceExposedByLoadBalancer(serviceName);
                break;
            default:
                break;
        }

        LOGGER.info("Service {} exposed: {}.", serviceName, serviceAddress);
        return serviceAddress;
    }

    private String resolveServiceExposedByClusterIp(String serviceName, String portName)
        throws TimeoutException {
        String namespace = config.getString(NAME_SPACE);
        String serviceSuffix = config.getString(SERVICE_SUFFIX);
        String serviceAddress = serviceName + K8SConstants.NAMESPACE_SEPARATOR + namespace;
        if (!StringUtils.isBlank(serviceSuffix)) {
            serviceAddress += K8SConstants.NAMESPACE_SEPARATOR + serviceSuffix;
        }

        LOGGER.info("Waiting for service {} to be exposed by cluster ip.", serviceName);
        Service service = getService(serviceName);
        //KubernetesUtils.waitForServiceNameResolved(config, true);

        int port = 0;
        for (ServicePort servicePort : service.getSpec().getPorts()) {
            if (servicePort.getName().equals(portName)) {
                port = servicePort.getPort();
                break;
            }
        }
        return serviceAddress + PORT_SEPARATOR + port;
    }

    private String resolveServiceExposedByNodePort(String serviceName, String portName)
        throws TimeoutException {
        LOGGER.info("Waiting for service {} to be exposed by node port.", serviceName);
        Service service = getService(serviceName);

        int nodePort = 0;
        for (ServicePort servicePort : service.getSpec().getPorts()) {
            if (servicePort.getName().equals(portName)) {
                nodePort = servicePort.getNodePort();
                break;
            }
        }
        return kubernetesClient.getKubernetesMasterHost() + PORT_SEPARATOR + nodePort;
    }

    private String resolveServiceExposedByLoadBalancer(String serviceName) {
        LOGGER.info("Waiting for service {} to be exposed by load balancer.", serviceName);
        List<String> ipList = new ArrayList<>();
        List<LoadBalancerIngress> ingressList;
        final long startTime = System.currentTimeMillis();
        Service service;
        while (true) {
            service = kubernetesClient.getService(serviceName);
            if (service != null) {
                ingressList = service.getStatus().getLoadBalancer().getIngress();
                if (ingressList != null && ingressList.size() > 0) {
                    for (LoadBalancerIngress ingress : ingressList) {
                        ipList.add(ingress.getIp());
                    }
                    break;
                }
            }
            if (System.currentTimeMillis() - startTime > 60000) {
                LOGGER.warn("Expose service took more than 60s, please check logs on the "
                    + "Kubernetes cluster.");
            }
            SleepUtils.sleepMilliSecond(DEFAULT_SLEEP_MS);
        }
        // just return the first one load balancer ip
        return ipList.get(0);
    }

    private Service getService(String serviceName) throws TimeoutException {
        Service service = kubernetesClient.getService(serviceName);
        final long startTime = System.currentTimeMillis();
        while (service == null) {
            long elapsedTime = System.currentTimeMillis() - startTime;
            if (elapsedTime > 60000) {
                LOGGER.warn("Get service {} took more than 60s, please check logs on Kubernetes"
                        + " cluster.", serviceName);
                if (elapsedTime > clientTimeoutMs) {
                    throw new TimeoutException("Resolve service " + serviceName + " timeout.");
                }
            }
            SleepUtils.sleepMilliSecond(DEFAULT_SLEEP_MS);
            service = kubernetesClient.getService(serviceName);
        }
        return service;
    }


}
