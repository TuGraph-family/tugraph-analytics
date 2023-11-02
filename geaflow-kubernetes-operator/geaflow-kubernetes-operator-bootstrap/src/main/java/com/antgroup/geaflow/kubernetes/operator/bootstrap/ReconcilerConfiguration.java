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

package com.antgroup.geaflow.kubernetes.operator.bootstrap;

import static com.antgroup.geaflow.cluster.k8s.config.KubernetesConfigKeys.CA_DATA;
import static com.antgroup.geaflow.cluster.k8s.config.KubernetesConfigKeys.CERT_DATA;
import static com.antgroup.geaflow.cluster.k8s.config.KubernetesConfigKeys.CERT_KEY;
import static com.antgroup.geaflow.cluster.k8s.config.KubernetesConfigKeys.MASTER_URL;
import static com.antgroup.geaflow.cluster.k8s.config.KubernetesConfigKeys.NAME_SPACE;

import com.antgroup.geaflow.cluster.k8s.clustermanager.KubernetesClientFactory;
import com.antgroup.geaflow.kubernetes.operator.core.job.GeaflowJobManager;
import com.antgroup.geaflow.kubernetes.operator.core.job.JobCRCache;
import com.antgroup.geaflow.kubernetes.operator.core.reconciler.GeaflowJobReconciler;
import com.antgroup.geaflow.kubernetes.operator.core.util.KubernetesUtil;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import io.javaoperatorsdk.operator.Operator;
import io.javaoperatorsdk.operator.api.reconciler.Reconciler;
import java.util.List;
import javax.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@Slf4j
public class ReconcilerConfiguration {

    @Value("${is_local_mode}")
    private Boolean isLocalMode;

    @Value("${k8s_master_url}")
    private String masterUrl;

    @Value("${k8s_namespace}")
    private String namespace;

    @Value("${k8s_client_cert_data}")
    private String clientCertData;

    @Value("${k8s_client_key_data}")
    private String clientKeyData;

    @Value("${k8s_ca_cert_data}")
    private String caCertData;

    @Autowired
    private GeaflowJobManager geaflowJobManager;

    @Autowired
    private JobCRCache jobCRCache;

    @PostConstruct
    private void postConstruct() {
        KubernetesClient kubernetesClient;
        if (isLocalMode != null && isLocalMode) {
            com.antgroup.geaflow.common.config.Configuration configuration =
                new com.antgroup.geaflow.common.config.Configuration();
            configuration.put(MASTER_URL, masterUrl);
            configuration.put(NAME_SPACE, namespace);
            configuration.put(CERT_KEY, clientKeyData);
            configuration.put(CERT_DATA, clientCertData);
            configuration.put(CA_DATA, caCertData);
            log.info("Operator is running in local host. Use custom k8s configuration: {}.", configuration);
            kubernetesClient = KubernetesClientFactory.create(configuration);
        } else {
            log.info(
                "Operator is deployed in kubernetes, use configuration of current k8s cluster.");
            kubernetesClient = new KubernetesClientBuilder().build();
        }
        KubernetesUtil.setKubernetesClient(kubernetesClient);
    }

    @Bean
    public GeaflowJobReconciler geaflowJobReconciler() {
        KubernetesClient kubernetesClient = KubernetesUtil.getKubernetesClient();
        return new GeaflowJobReconciler(kubernetesClient, geaflowJobManager, jobCRCache);
    }

    @Bean(initMethod = "start", destroyMethod = "stop")
    @SuppressWarnings("rawtypes")
    public Operator operator(List<Reconciler> controllers) {
        Operator operator = new Operator();
        controllers.forEach(operator::register);
        return operator;
    }
}
