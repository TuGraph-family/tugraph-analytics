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

package com.antgroup.geaflow.console.core.model.job.config;

import com.antgroup.geaflow.console.common.util.exception.GeaflowException;
import com.antgroup.geaflow.console.core.model.config.ConfigValueBehavior;
import com.antgroup.geaflow.console.core.model.config.GeaflowConfigClass;
import com.antgroup.geaflow.console.core.model.config.GeaflowConfigKey;
import com.antgroup.geaflow.console.core.model.config.GeaflowConfigValue;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
public class K8sClientArgsClass extends GeaflowConfigClass {

    @GeaflowConfigKey(value = "kubernetes.user.class.args", comment = "i18n.key.engine.params.json")
    @GeaflowConfigValue(required = true, behavior = ConfigValueBehavior.JSON)
    private GeaflowArgsClass geaflowArgs;

    @GeaflowConfigKey(value = "kubernetes.user.main.class", comment = "i18n.key.main.class")
    @GeaflowConfigValue(required = true)
    private String mainClass;

    @GeaflowConfigKey(value = "geaflow.job.cluster.id", comment = "i18n.key.running.job.id")
    @GeaflowConfigValue(required = true)
    private String runtimeTaskId;

    @GeaflowConfigKey(value = "clusterArgs", comment = "i18n.key.cluster.args")
    @GeaflowConfigValue(required = true, behavior = ConfigValueBehavior.FLATTED)
    private K8SClusterArgsClass clusterArgs;

    @GeaflowConfigKey(value = "geaflow.gw.endpoint", comment = "i18n.key.k8s.server.url")
    private String gateway;

    @GeaflowConfigKey(value = "geaflow.dsl.catalog.token.key", comment = "i18n.key.api.token")
    private String token;

    public K8sClientArgsClass(GeaflowArgsClass geaflowArgs, String mainClass) {
        this.geaflowArgs = geaflowArgs;
        this.mainClass = mainClass;
        this.gateway = geaflowArgs.getSystemArgs().getGateway();
        this.token = geaflowArgs.getSystemArgs().getTaskToken();
        this.runtimeTaskId = geaflowArgs.getSystemArgs().getRuntimeTaskId();

        ClusterArgsClass clusterArgs = geaflowArgs.getClusterArgs();
        if (clusterArgs instanceof K8SClusterArgsClass) {
            this.clusterArgs = (K8SClusterArgsClass) clusterArgs;
        } else {
            throw new GeaflowException("Invalid clusterArgs type {}", clusterArgs.getClass());
        }
    }
}
