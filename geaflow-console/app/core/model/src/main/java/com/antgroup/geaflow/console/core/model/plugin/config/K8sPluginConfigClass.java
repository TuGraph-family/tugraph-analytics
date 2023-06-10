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

package com.antgroup.geaflow.console.core.model.plugin.config;

import com.antgroup.geaflow.console.common.util.NetworkUtil;
import com.antgroup.geaflow.console.common.util.type.GeaflowPluginType;
import com.antgroup.geaflow.console.core.model.config.GeaflowConfigKey;
import com.antgroup.geaflow.console.core.model.config.GeaflowConfigValue;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Getter
@Setter
public class K8sPluginConfigClass extends PluginConfigClass {

    @GeaflowConfigKey(value = "kubernetes.master.url", comment = "API服务地址")
    @GeaflowConfigValue(required = true, defaultValue = "https://0.0.0.0:6443")
    private String masterUrl;

    @GeaflowConfigKey(value = "kubernetes.container.image", comment = "GeaFlow镜像仓库地址")
    @GeaflowConfigValue(required = true, defaultValue = "tugraph/geaflow:0.1")
    private String imageUrl;

    @GeaflowConfigKey(value = "kubernetes.service.account", comment = "API服务账户名")
    @GeaflowConfigValue(defaultValue = "geaflow")
    private String serviceAccount;

    @GeaflowConfigKey(value = "kubernetes.service.exposed.type", comment = "API服务类型")
    @GeaflowConfigValue(defaultValue = "NODE_PORT")
    private String serviceType;

    @GeaflowConfigKey(value = "kubernetes.namespace", comment = "Namespace")
    @GeaflowConfigValue(defaultValue = "default")
    private String namespace;

    @GeaflowConfigKey(value = "kubernetes.cert.data", comment = "Client Cert Data")
    private String certData;

    @GeaflowConfigKey(value = "kubernetes.cert.key", comment = "Client Cert key")
    private String certKey;

    @GeaflowConfigKey(value = "kubernetes.ca.data", comment = "Cluster CA Data")
    private String caData;

    @GeaflowConfigKey(value = "kubernetes.connection.retry.times", comment = "重试次数")
    @GeaflowConfigValue(defaultValue = "100")
    private Integer retryTimes;

    @GeaflowConfigKey(value = "kubernetes.cluster.name", comment = "集群名")
    private String clusterName;

    @GeaflowConfigKey(value = "kubernetes.pod.user.labels", comment = "Pod用户标签")
    private String podUserLabels;

    @GeaflowConfigKey(value = "kubernetes.service.suffix", comment = "API服务后缀")
    private String serviceSuffix;

    @GeaflowConfigKey(value = "kubernetes.resource.storage.limit.size", comment = "存储上限")
    @GeaflowConfigValue(defaultValue = "10Gi")
    private String storageLimit;

    @GeaflowConfigKey(value = "kubernetes.geaflow.cluster.timeout.ms", comment = "Client超时时间")
    @GeaflowConfigValue(defaultValue = "300000")
    private Integer clientTimeout;

    @GeaflowConfigKey(value = "kubernetes.container.image.pullPolicy", comment = "镜像拉取策略")
    @GeaflowConfigValue(defaultValue = "Always")
    private String pullPolicy;

    public K8sPluginConfigClass() {
        super(GeaflowPluginType.K8S);
    }

    @Override
    public void testConnection() {
        NetworkUtil.testUrl(masterUrl);
    }
}
