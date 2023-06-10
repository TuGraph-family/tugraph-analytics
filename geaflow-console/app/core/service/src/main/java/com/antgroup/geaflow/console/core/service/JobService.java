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

package com.antgroup.geaflow.console.core.service;

import com.antgroup.geaflow.console.common.dal.dao.IdDao;
import com.antgroup.geaflow.console.common.dal.dao.JobDao;
import com.antgroup.geaflow.console.common.dal.dao.JobResourceMappingDao;
import com.antgroup.geaflow.console.common.dal.entity.JobEntity;
import com.antgroup.geaflow.console.common.dal.entity.JobResourceMappingEntity;
import com.antgroup.geaflow.console.common.dal.model.JobSearch;
import com.antgroup.geaflow.console.common.service.integration.engine.CompileResult;
import com.antgroup.geaflow.console.common.service.integration.engine.GraphInfo;
import com.antgroup.geaflow.console.common.service.integration.engine.TableInfo;
import com.antgroup.geaflow.console.common.util.ListUtil;
import com.antgroup.geaflow.console.common.util.context.ContextHolder;
import com.antgroup.geaflow.console.common.util.type.GeaflowAuthorityType;
import com.antgroup.geaflow.console.common.util.type.GeaflowResourceType;
import com.antgroup.geaflow.console.core.model.data.GeaflowData;
import com.antgroup.geaflow.console.core.model.data.GeaflowFunction;
import com.antgroup.geaflow.console.core.model.data.GeaflowGraph;
import com.antgroup.geaflow.console.core.model.data.GeaflowStruct;
import com.antgroup.geaflow.console.core.model.job.GeaflowJob;
import com.antgroup.geaflow.console.core.model.job.GeaflowProcessJob;
import com.antgroup.geaflow.console.core.model.security.GeaflowAuthorization;
import com.antgroup.geaflow.console.core.model.version.GeaflowVersion;
import com.antgroup.geaflow.console.core.service.converter.JobConverter;
import com.antgroup.geaflow.console.core.service.converter.NameConverter;
import com.antgroup.geaflow.console.core.service.factory.GeaflowDataFactory;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class JobService extends IdService<GeaflowJob, JobEntity, JobSearch> {

    private final Map<GeaflowResourceType, DataService> serviceMap = new HashMap<>();
    @Autowired
    private JobDao jobDao;

    @Autowired
    private JobConverter jobConverter;

    @Autowired
    private JobResourceMappingDao jobResourceMappingDao;

    @Autowired
    private GraphService graphService;

    @Autowired
    private TableService tableService;

    @Autowired
    private VertexService vertexService;

    @Autowired
    private EdgeService edgeService;

    @Autowired
    private FunctionService functionService;

    @Autowired
    private VersionService versionService;

    @Autowired
    private ReleaseService releaseService;

    @Autowired
    private InstanceService instanceService;

    @Autowired
    private AuthorizationService authorizationService;

    @Autowired
    private TaskService taskService;

    protected IdDao<JobEntity, JobSearch> getDao() {
        return jobDao;
    }

    @Override
    protected NameConverter<GeaflowJob, JobEntity> getConverter() {
        return jobConverter;
    }

    @Override
    protected List<GeaflowJob> parse(List<JobEntity> jobEntities) {
        return jobEntities.stream().map(e -> {
            List<GeaflowStruct> structs = getJobStructs(e.getId());
            List<GeaflowGraph> graphs = getJobGraphs(e.getId());
            List<GeaflowFunction> functions = getJobFunctions(e.getId());
            return jobConverter.convert(e, structs, graphs, functions);
        }).collect(Collectors.toList());
    }

    @Override
    @Transactional
    public List<String> create(List<GeaflowJob> models) {
        // compile processJob
        GeaflowVersion version = versionService.getDefaultVersion();
        for (GeaflowJob job : models) {
            parseUserCode(job, version);
        }
        List<String> ids = super.create(models);
        // save resourceMappings
        for (GeaflowJob job : models) {
            createJobResources(job.getId(), job.getStructs(), job.getGraphs(), job.getFunctions());
        }
        // save authorizations
        List<GeaflowAuthorization> authorizations = ListUtil.convert(ids,
            id -> new GeaflowAuthorization(ContextHolder.get().getUserId(), GeaflowAuthorityType.ALL, GeaflowResourceType.JOB, id));
        authorizationService.create(authorizations);
        return ids;
    }

    @Override
    public boolean update(List<GeaflowJob> jobs) {
        GeaflowVersion version = versionService.getDefaultVersion();
        for (GeaflowJob newJob : jobs) {
            String jobId = newJob.getId();
            GeaflowJob oldJob = this.get(jobId);

            parseUserCode(newJob, version);
            // calculate subset of graphs
            List<GeaflowGraph> oldGraphs = oldJob.getGraphs();
            List<GeaflowGraph> newGraphs = newJob.getGraphs();

            List<GeaflowGraph> addGraphs = ListUtil.diff(newGraphs, oldGraphs, this::getResourceKey);
            List<GeaflowGraph> removeGraphs = ListUtil.diff(oldGraphs, newGraphs, this::getResourceKey);

            // calculate subset of structs
            List<GeaflowStruct> oldStructs = oldJob.getStructs();
            List<GeaflowStruct> newStructs = newJob.getStructs();

            List<GeaflowStruct> addStructs = ListUtil.diff(newStructs, oldStructs, this::getResourceKey);
            List<GeaflowStruct> removeStructs = ListUtil.diff(oldStructs, newStructs, this::getResourceKey);

            // add resources
            createJobResources(jobId, addStructs, addGraphs, new ArrayList<>());
            removeJobResources(jobId, removeStructs, removeGraphs, new ArrayList<>());
        }

        return super.update(jobs);
    }

    public void dropResources(List<String> jobIds) {
        jobResourceMappingDao.dropByJobIds(jobIds);
    }

    @PostConstruct
    public void init() {
        serviceMap.put(GeaflowResourceType.VERTEX, vertexService);
        serviceMap.put(GeaflowResourceType.EDGE, edgeService);
        serviceMap.put(GeaflowResourceType.TABLE, tableService);
        serviceMap.put(GeaflowResourceType.GRAPH, graphService);
        serviceMap.put(GeaflowResourceType.FUNCTION, functionService);
    }

    public DataService getResourceService(GeaflowResourceType resourceType) {
        return serviceMap.get(resourceType);
    }

    private void createJobResources(String jobId, List<GeaflowStruct> structs, List<GeaflowGraph> graphs,
                                    List<GeaflowFunction> functions) {
        List<JobResourceMappingEntity> entities = new ArrayList<>();

        structs.forEach(e -> {
            JobResourceMappingEntity entity = new JobResourceMappingEntity(jobId, e.getName(),
                GeaflowResourceType.valueOf(e.getType().name()), e.getInstanceId());
            entities.add(entity);
        });

        graphs.forEach(e -> {
            JobResourceMappingEntity entity = new JobResourceMappingEntity(jobId, e.getName(), GeaflowResourceType.GRAPH,
                e.getInstanceId());
            entities.add(entity);
        });

        functions.forEach(e -> {
            JobResourceMappingEntity entity = new JobResourceMappingEntity(jobId, e.getName(), GeaflowResourceType.FUNCTION,
                e.getInstanceId());
            entities.add(entity);
        });

        if (!entities.isEmpty()) {
            jobResourceMappingDao.create(entities);
        }
    }

    private List<GeaflowGraph> getJobGraphs(String id) {
        return getResourcesByJobId(id, GeaflowResourceType.GRAPH);
    }

    private List<GeaflowFunction> getJobFunctions(String id) {
        return getResourcesByJobId(id, GeaflowResourceType.FUNCTION);
    }

    private List<GeaflowStruct> getJobStructs(String jobId) {
        List<GeaflowStruct> res = new ArrayList<>();
        res.addAll(getResourcesByJobId(jobId, GeaflowResourceType.TABLE));
        res.addAll(getResourcesByJobId(jobId, GeaflowResourceType.VERTEX));
        res.addAll(getResourcesByJobId(jobId, GeaflowResourceType.EDGE));
        return res;
    }

    private void parseUserCode(GeaflowJob job, GeaflowVersion version) {
        if (job instanceof GeaflowProcessJob) {
            // compile the code
            GeaflowProcessJob tmp = (GeaflowProcessJob) job;
            CompileResult result = releaseService.compile(job, version, null);
            // set job structs and graphs
            Set<GraphInfo> graphInfos = result.getSourceGraphs();
            graphInfos.addAll(result.getTargetGraphs());

            Set<TableInfo> tableInfos = result.getSourceTables();
            tableInfos.addAll(result.getTargetTables());

            List<GeaflowGraph> graphs = getByGraphInfo(graphInfos, GeaflowResourceType.GRAPH);
            List<GeaflowStruct> tables = getByTableInfo(tableInfos, GeaflowResourceType.TABLE);

            tmp.setStructs(tables);
            tmp.setGraph(graphs);
        }
    }

    private <T extends GeaflowData> List<T> getByTableInfo(Set<TableInfo> infos, GeaflowResourceType resourceType) {
        return infos.stream().map(info -> {
            String instanceId = instanceService.getIdByName(info.getInstanceName());
            return (T) getResourceOrCreate(info.getTableName(), instanceId, resourceType);
        }).collect(Collectors.toList());
    }

    private <T extends GeaflowData> List<T> getByGraphInfo(Set<GraphInfo> infos, GeaflowResourceType resourceType) {
        return infos.stream().map(info -> {
            String instanceId = instanceService.getIdByName(info.getInstanceName());
            return (T) getResourceOrCreate(info.getGraphName(), instanceId, resourceType);
        }).collect(Collectors.toList());
    }

    private <T extends GeaflowData> List<T> getResourcesByJobId(String jobId, GeaflowResourceType resourceType) {
        List<JobResourceMappingEntity> entities = jobResourceMappingDao.getResourcesByJobId(jobId, resourceType);
        return ListUtil.convert(entities, e -> getResourceOrCreate(e.getResourceName(), e.getInstanceId(), resourceType));
    }

    private <T extends GeaflowData> T getResourceOrCreate(String resourceName, String instanceId, GeaflowResourceType resourceType) {
        // create a model only with name if no resource in database.
        T data = (T) getResourceService(resourceType).getByName(instanceId, resourceName);
        if (data == null) {
            data = (T) GeaflowDataFactory.get(resourceName, null, instanceId, resourceType);
        }
        return data;
    }

    private void removeJobResources(String jobId, List<GeaflowStruct> removeStructs, List<GeaflowGraph> removeGraphs,
                                    List<GeaflowFunction> removeFunctions) {
        List<JobResourceMappingEntity> removeEntities = ListUtil.convert(removeGraphs,
            e -> new JobResourceMappingEntity(jobId, e.getName(), GeaflowResourceType.GRAPH, e.getInstanceId()));
        removeEntities.addAll(ListUtil.convert(removeStructs,
            e -> new JobResourceMappingEntity(jobId, e.getName(), GeaflowResourceType.valueOf(e.getType().name()), e.getInstanceId())));
        removeEntities.addAll(ListUtil.convert(removeFunctions,
            e -> new JobResourceMappingEntity(jobId, e.getName(), GeaflowResourceType.FUNCTION, e.getInstanceId())));
        jobResourceMappingDao.removeJobResources(removeEntities);
    }

    private String getResourceKey(GeaflowData e) {
        return e.getInstanceId() + "-" + e.getName();
    }

}

