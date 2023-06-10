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

package com.antgroup.geaflow.cluster.k8s.entrypoint;

import com.antgroup.geaflow.cluster.k8s.clustermanager.GeaflowKubeClient;
import com.antgroup.geaflow.cluster.k8s.clustermanager.KubernetesResourceBuilder;
import com.antgroup.geaflow.cluster.k8s.config.KubernetesConfig;
import com.antgroup.geaflow.cluster.k8s.config.KubernetesConfig.ServiceExposedType;
import com.antgroup.geaflow.cluster.k8s.config.KubernetesConfigKeys;
import com.antgroup.geaflow.cluster.k8s.config.KubernetesMasterParam;
import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys;
import com.antgroup.geaflow.common.config.keys.FrameworkConfigKeys;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.server.mock.KubernetesServer;
import java.util.HashMap;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;

public class KubernetesTestBase {

    private final KubernetesServer server = new KubernetesServer(false, true);
    protected KubernetesClient kubernetesClient;
    protected String masterUrl;
    protected String clusterId;
    protected Configuration configuration;
    protected GeaflowKubeClient geaflowKubeClient;

    @BeforeMethod
    public void setUp() {
        server.before();
        kubernetesClient = server.getClient();
        masterUrl = kubernetesClient.getMasterUrl().toString();
        clusterId = "123";

        configuration = new Configuration();
        configuration.put(FrameworkConfigKeys.SYSTEM_STATE_BACKEND_TYPE, "memory");
        configuration.put(ExecutionConfigKeys.REPORTER_LIST, "slf4j");
        configuration.put(ExecutionConfigKeys.HA_SERVICE_TYPE, "memory");
        configuration.put(ExecutionConfigKeys.JOB_UNIQUE_ID, "1");
        configuration.put(ExecutionConfigKeys.CLUSTER_ID, clusterId);
        configuration.put(KubernetesConfigKeys.MASTER_URL, masterUrl);
        configuration.put(KubernetesConfig.CLUSTER_START_TIME,
            String.valueOf(System.currentTimeMillis()));
        configuration.setMasterId("mockMaster");

        geaflowKubeClient = new GeaflowKubeClient(configuration, masterUrl);
        createService(configuration);
    }

    @AfterMethod
    public void destroy() {
        server.after();
    }

    private void createService(Configuration configuration) {
        KubernetesMasterParam param = new KubernetesMasterParam(configuration);
        Service service = KubernetesResourceBuilder
            .createService(param.getServiceName(clusterId), ServiceExposedType.CLUSTER_IP,
                new HashMap<>(), null, param);
        geaflowKubeClient.createService(service);
    }
}
