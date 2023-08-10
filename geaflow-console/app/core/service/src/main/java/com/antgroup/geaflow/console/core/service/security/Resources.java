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

package com.antgroup.geaflow.console.core.service.security;

import com.antgroup.geaflow.console.common.util.type.GeaflowResourceType;
import com.antgroup.geaflow.console.core.model.security.resource.EdgeResource;
import com.antgroup.geaflow.console.core.model.security.resource.FunctionResource;
import com.antgroup.geaflow.console.core.model.security.resource.GraphResource;
import com.antgroup.geaflow.console.core.model.security.resource.InstanceResource;
import com.antgroup.geaflow.console.core.model.security.resource.JobResource;
import com.antgroup.geaflow.console.core.model.security.resource.TableResource;
import com.antgroup.geaflow.console.core.model.security.resource.TaskResource;
import com.antgroup.geaflow.console.core.model.security.resource.TenantResource;
import com.antgroup.geaflow.console.core.model.security.resource.VertexResource;
import com.antgroup.geaflow.console.core.model.security.resource.ViewResource;

public class Resources {

    private static ResourceFactory getResourceFactory() {
        return ResourceFactory.getInstance();
    }

    public static TenantResource tenant(String tenantId) {
        return getResourceFactory().build(GeaflowResourceType.TENANT, tenantId);
    }

    public static InstanceResource instance(String instanceId) {
        return getResourceFactory().build(GeaflowResourceType.INSTANCE, instanceId);
    }

    public static TableResource table(String tableId) {
        return getResourceFactory().build(GeaflowResourceType.TABLE, tableId);
    }

    public static ViewResource view(String viewId) {
        return getResourceFactory().build(GeaflowResourceType.VIEW, viewId);
    }

    public static FunctionResource function(String functionId) {
        return getResourceFactory().build(GeaflowResourceType.FUNCTION, functionId);
    }

    public static VertexResource vertex(String vertexId) {
        return getResourceFactory().build(GeaflowResourceType.VERTEX, vertexId);
    }

    public static EdgeResource edge(String edgeId) {
        return getResourceFactory().build(GeaflowResourceType.EDGE, edgeId);
    }

    public static GraphResource graph(String graphId) {
        return getResourceFactory().build(GeaflowResourceType.GRAPH, graphId);
    }

    public static JobResource job(String jobId) {
        return getResourceFactory().build(GeaflowResourceType.JOB, jobId);
    }

    public static TaskResource task(String taskId) {
        return getResourceFactory().build(GeaflowResourceType.TASK, taskId);
    }
}
