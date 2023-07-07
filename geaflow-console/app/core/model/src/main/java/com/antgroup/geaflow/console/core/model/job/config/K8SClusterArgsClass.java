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

import com.antgroup.geaflow.console.core.model.config.ConfigValueBehavior;
import com.antgroup.geaflow.console.core.model.config.GeaflowConfigKey;
import com.antgroup.geaflow.console.core.model.config.GeaflowConfigValue;
import com.antgroup.geaflow.console.core.model.plugin.config.K8sPluginConfigClass;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class K8SClusterArgsClass extends ClusterArgsClass {

    @GeaflowConfigKey(value = "clusterConfig", comment = "i18n.key.k8s.cluster.config")
    @GeaflowConfigValue(required = true, behavior = ConfigValueBehavior.FLATTED)
    private K8sPluginConfigClass clusterConfig;

    @GeaflowConfigKey(value = "kubernetes.engine.jar.files", comment = "i18n.key.engine.jar.list")
    private String engineJarUrls;

    @GeaflowConfigKey(value = "kubernetes.user.jar.files", comment = "i18n.key.task.jar.list")
    private String taskJarUrls;

}
