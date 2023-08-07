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

import com.antgroup.geaflow.console.common.dal.dao.EdgeDao;
import com.antgroup.geaflow.console.common.dal.dao.FunctionDao;
import com.antgroup.geaflow.console.common.dal.dao.GraphDao;
import com.antgroup.geaflow.console.common.dal.dao.InstanceDao;
import com.antgroup.geaflow.console.common.dal.dao.JobDao;
import com.antgroup.geaflow.console.common.dal.dao.TableDao;
import com.antgroup.geaflow.console.common.dal.dao.TaskDao;
import com.antgroup.geaflow.console.common.dal.dao.VertexDao;
import com.antgroup.geaflow.console.common.dal.dao.ViewDao;
import com.antgroup.geaflow.console.common.dal.entity.EdgeEntity;
import com.antgroup.geaflow.console.common.dal.entity.FunctionEntity;
import com.antgroup.geaflow.console.common.dal.entity.GraphEntity;
import com.antgroup.geaflow.console.common.dal.entity.InstanceEntity;
import com.antgroup.geaflow.console.common.dal.entity.JobEntity;
import com.antgroup.geaflow.console.common.dal.entity.TableEntity;
import com.antgroup.geaflow.console.common.dal.entity.TaskEntity;
import com.antgroup.geaflow.console.common.dal.entity.VertexEntity;
import com.antgroup.geaflow.console.common.dal.entity.ViewEntity;
import com.antgroup.geaflow.console.common.util.exception.GeaflowException;
import com.antgroup.geaflow.console.common.util.exception.GeaflowIllegalException;
import com.antgroup.geaflow.console.common.util.type.GeaflowResourceType;
import com.antgroup.geaflow.console.core.model.security.resource.GeaflowResource;
import com.antgroup.geaflow.console.core.model.security.resource.GraphResource;
import com.antgroup.geaflow.console.core.model.security.resource.InstanceResource;
import com.antgroup.geaflow.console.core.model.security.resource.JobResource;
import com.antgroup.geaflow.console.core.model.security.resource.TableResource;
import com.antgroup.geaflow.console.core.model.security.resource.TaskResource;
import com.antgroup.geaflow.console.core.model.security.resource.TenantResource;
import com.antgroup.geaflow.console.core.model.security.resource.ViewResource;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class ResourceFactory implements InitializingBean {

    private static ResourceFactory INSTANCE;

    @Autowired
    private InstanceDao instanceDao;

    @Autowired
    private TableDao tableDao;

    @Autowired
    private ViewDao viewDao;

    @Autowired
    private FunctionDao functionDao;

    @Autowired
    private VertexDao vertexDao;

    @Autowired
    private EdgeDao edgeDao;

    @Autowired
    private GraphDao graphDao;

    @Autowired
    private JobDao jobDao;

    @Autowired
    private TaskDao taskDao;

    protected static ResourceFactory getInstance() {
        if (INSTANCE == null) {
            throw new GeaflowException("{} is not ready", ResourceFactory.class.getSimpleName());
        }
        return INSTANCE;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        INSTANCE = this;
    }

    public <T extends GeaflowResource> T build(GeaflowResourceType resourceType, String resourceId) {
        GeaflowResource resource;
        try {
            switch (resourceType) {
                case TENANT:
                    resource = new TenantResource(resourceId);
                    break;

                case INSTANCE:
                    InstanceEntity instance = instanceDao.get(resourceId);
                    resource = new InstanceResource(instance.getTenantId(), instance.getId());
                    break;

                case TABLE:
                    TableEntity table = tableDao.get(resourceId);
                    resource = new TableResource(table.getTenantId(), table.getInstanceId(), table.getId());
                    break;

                case VIEW:
                    ViewEntity view = viewDao.get(resourceId);
                    resource = new ViewResource(view.getTenantId(), view.getInstanceId(), view.getId());
                    break;

                case FUNCTION:
                    FunctionEntity function = functionDao.get(resourceId);
                    resource = new ViewResource(function.getTenantId(), function.getInstanceId(), function.getId());
                    break;

                case VERTEX:
                    VertexEntity vertex = vertexDao.get(resourceId);
                    resource = new GraphResource(vertex.getTenantId(), vertex.getInstanceId(), vertex.getId());
                    break;

                case EDGE:
                    EdgeEntity edge = edgeDao.get(resourceId);
                    resource = new GraphResource(edge.getTenantId(), edge.getInstanceId(), edge.getId());
                    break;

                case GRAPH:
                    GraphEntity graph = graphDao.get(resourceId);
                    resource = new GraphResource(graph.getTenantId(), graph.getInstanceId(), graph.getId());
                    break;

                case JOB:
                    JobEntity job = jobDao.get(resourceId);
                    resource = new JobResource(job.getTenantId(), job.getInstanceId(), job.getId());
                    break;

                case TASK:
                    TaskEntity task = taskDao.get(resourceId);
                    resource = new TaskResource(build(GeaflowResourceType.JOB, task.getJobId()), task.getId());
                    break;

                default:
                    throw new GeaflowException("Resource type {} not supported", resourceType);
            }

            return (T) resource;

        } catch (Exception e) {
            throw new GeaflowIllegalException("Build resource {} {} failed", resourceType, resourceId, e);
        }
    }
}
