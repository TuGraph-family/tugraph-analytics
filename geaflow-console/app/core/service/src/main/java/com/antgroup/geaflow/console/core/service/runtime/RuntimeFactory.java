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

package com.antgroup.geaflow.console.core.service.runtime;

import com.antgroup.geaflow.console.common.util.exception.GeaflowException;
import com.antgroup.geaflow.console.common.util.type.GeaflowPluginType;
import com.antgroup.geaflow.console.core.model.task.GeaflowTask;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

@Component
public class RuntimeFactory {

    @Autowired
    ApplicationContext context;

    public GeaflowRuntime getRuntime(GeaflowTask task) {
        GeaflowPluginType type = task.getRelease().getCluster().getType();

        Class<? extends GeaflowRuntime> runtimeClass;
        switch (type) {
            case CONTAINER:
                runtimeClass = ContainerRuntime.class;
                break;
            case K8S:
                runtimeClass = K8sRuntime.class;
                break;
            default:
                throw new GeaflowException("Unsupported runtime type {}", type);
        }

        return context.getBean(runtimeClass);
    }

}
