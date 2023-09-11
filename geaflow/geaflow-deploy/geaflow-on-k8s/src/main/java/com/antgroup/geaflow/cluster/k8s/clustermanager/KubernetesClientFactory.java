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

import static com.antgroup.geaflow.cluster.k8s.config.KubernetesConfigKeys.CA_DATA;
import static com.antgroup.geaflow.cluster.k8s.config.KubernetesConfigKeys.CERT_DATA;
import static com.antgroup.geaflow.cluster.k8s.config.KubernetesConfigKeys.CERT_KEY;
import static com.antgroup.geaflow.cluster.k8s.config.KubernetesConfigKeys.CLIENT_KEY_ALGO;
import static com.antgroup.geaflow.cluster.k8s.config.KubernetesConfigKeys.MASTER_URL;
import static com.antgroup.geaflow.cluster.k8s.config.KubernetesConfigKeys.NAME_SPACE;
import static com.antgroup.geaflow.cluster.k8s.config.KubernetesConfigKeys.PING_INTERVAL_MS;

import com.antgroup.geaflow.common.config.Configuration;
import io.fabric8.kubernetes.client.ConfigBuilder;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.utils.HttpClientUtils;
import okhttp3.Dispatcher;
import okhttp3.OkHttpClient;
import org.apache.commons.lang3.StringUtils;

/**
 * Builder for Kubernetes clients.
 */
public class KubernetesClientFactory {

    public static KubernetesClient create(Configuration config) {
        String masterUrl = config.getString(MASTER_URL);
        return create(config, masterUrl);
    }

    public static KubernetesClient create(Configuration config, String masterUrl) {
        String namespace = config.getString(NAME_SPACE);
        long pingInterval = config.getLong(PING_INTERVAL_MS);

        ConfigBuilder clientConfig = new ConfigBuilder()
            .withApiVersion("v1")
            .withMasterUrl(masterUrl)
            .withWebsocketPingInterval(pingInterval)
            .withNamespace(namespace);

        clientConfig.withTrustCerts(true);
        String certKey = config.getString(CERT_KEY);
        if (!StringUtils.isBlank(certKey)) {
            clientConfig.withClientKeyData(certKey);
        }

        String certData = config.getString(CERT_DATA);
        if (!StringUtils.isBlank(certData)) {
            clientConfig.withClientCertData(certData);
        }

        String caData = config.getString(CA_DATA);
        if (!StringUtils.isBlank(caData)) {
            clientConfig.withCaCertData(certData);
        }

        String certKeyAlgo = config.getString(CLIENT_KEY_ALGO);
        if (!StringUtils.isBlank(certKeyAlgo)) {
            clientConfig.withClientKeyAlgo(certKeyAlgo);
        }

        Dispatcher dispatcher = new Dispatcher();
        OkHttpClient httpClient = HttpClientUtils.createHttpClient(clientConfig.build())
            .newBuilder().dispatcher(dispatcher).build();

        return new DefaultKubernetesClient(httpClient, clientConfig.build());
    }

}

