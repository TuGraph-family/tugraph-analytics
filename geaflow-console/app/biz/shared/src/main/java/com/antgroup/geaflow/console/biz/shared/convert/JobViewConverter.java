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

package com.antgroup.geaflow.console.biz.shared.convert;

import com.antgroup.geaflow.console.biz.shared.view.FunctionView;
import com.antgroup.geaflow.console.biz.shared.view.GraphView;
import com.antgroup.geaflow.console.biz.shared.view.JobView;
import com.antgroup.geaflow.console.biz.shared.view.StructView;
import com.antgroup.geaflow.console.common.util.ListUtil;
import com.antgroup.geaflow.console.common.util.exception.GeaflowException;
import com.antgroup.geaflow.console.common.util.type.GeaflowJobType;
import com.antgroup.geaflow.console.common.util.type.GeaflowStructType;
import com.antgroup.geaflow.console.core.model.code.GeaflowCode;
import com.antgroup.geaflow.console.core.model.data.GeaflowFunction;
import com.antgroup.geaflow.console.core.model.data.GeaflowGraph;
import com.antgroup.geaflow.console.core.model.data.GeaflowStruct;
import com.antgroup.geaflow.console.core.model.file.GeaflowRemoteFile;
import com.antgroup.geaflow.console.core.model.job.GeaflowCustomJob;
import com.antgroup.geaflow.console.core.model.job.GeaflowIntegrateJob;
import com.antgroup.geaflow.console.core.model.job.GeaflowJob;
import com.antgroup.geaflow.console.core.model.job.GeaflowProcessJob;
import com.antgroup.geaflow.console.core.service.InstanceService;
import com.google.common.base.Preconditions;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class JobViewConverter extends NameViewConverter<GeaflowJob, JobView> {

    @Autowired
    private GraphViewConverter graphViewConverter;

    @Autowired
    private VertexViewConverter vertexViewConverter;

    @Autowired
    private EdgeViewConverter edgeViewConverter;

    @Autowired
    private TableViewConverter tableViewConverter;

    @Autowired
    private RemoteFileViewConverter remoteFileViewConverter;

    @Autowired
    private FunctionViewConverter functionViewConverter;

    @Autowired
    private InstanceService instanceService;

    private final Map<GeaflowStructType, StructViewConverter> converterMap = new HashMap<>();

    @PostConstruct
    public void init() {
        converterMap.put(GeaflowStructType.VERTEX, vertexViewConverter);
        converterMap.put(GeaflowStructType.EDGE, edgeViewConverter);
        converterMap.put(GeaflowStructType.TABLE, tableViewConverter);
    }

    @Override
    public void merge(JobView view, JobView updateView) {
        super.merge(view, updateView);
        switch (view.getType()) {
            case PROCESS:
                Optional.ofNullable(updateView.getUserCode()).ifPresent(view::setUserCode);
                break;
            case CUSTOM:
                Optional.ofNullable(updateView.getEntryClass()).ifPresent(view::setEntryClass);
                Optional.ofNullable(updateView.getJarPackage()).ifPresent(view::setJarPackage);
                break;
            default:
                throw new GeaflowException("Unsupported job type: {}", view.getType());
        }
    }

    @Override
    protected JobView modelToView(GeaflowJob model) {
        JobView jobView = super.modelToView(model);
        jobView.setUserCode(Optional.ofNullable(model.getUserCode()).map(GeaflowCode::getText).orElse(null));

        List<GraphView> graphs = ListUtil.convert(model.getGraphs(), e -> graphViewConverter.convert(e));

        List<StructView> structs = ListUtil.convert(model.getStructs(), e -> (StructView) converterMap.get(e.getType()).convert(e));

        jobView.setStructs(structs);
        jobView.setGraphs(graphs);
        jobView.setStructMappings(model.getStructMappings());

        List<FunctionView> functions = ListUtil.convert(model.getFunctions(), e -> functionViewConverter.convert(e));
        jobView.setFunctions(functions);
        jobView.setType(model.getType());
        jobView.setInstanceId(model.getInstanceId());
        jobView.setInstanceName(instanceService.get(model.getInstanceId()).getName());
        jobView.setEntryClass(model.getEntryClass());
        jobView.setJarPackage(Optional.ofNullable(model.getJarPackage()).map(e -> remoteFileViewConverter.convert(e)).orElse(null));
        return jobView;
    }

    public GeaflowJob convert(JobView view, List<GeaflowStruct> structs, List<GeaflowGraph> graphs,
                              List<GeaflowFunction> functions, GeaflowRemoteFile jarFile) {
        GeaflowJobType jobType = view.getType();
        GeaflowJob job;
        switch (jobType) {
            case INTEGRATE:
                GeaflowIntegrateJob integrateJob = (GeaflowIntegrateJob) viewToModel(view, GeaflowIntegrateJob.class);
                Map<String, Map<String, Map<String, String>>> structMappings = view.getStructMappings();
                Preconditions.checkNotNull(structMappings);
                integrateJob.setStructMappings(structMappings);
                integrateJob.setGraph(graphs);
                integrateJob.setStructs(structs);
                job = integrateJob;
                break;
            case PROCESS:
                GeaflowProcessJob processJob = (GeaflowProcessJob) viewToModel(view, GeaflowProcessJob.class);
                GeaflowCode geaflowCode = new GeaflowCode(view.getUserCode());
                processJob.setUserCode(geaflowCode);
                processJob.setFunctions(functions);
                job = processJob;
                break;
            case CUSTOM:
                GeaflowCustomJob customJob = (GeaflowCustomJob) viewToModel(view, GeaflowCustomJob.class);
                customJob.setEntryClass(view.getEntryClass());
                customJob.setJarPackage(jarFile);
                job = customJob;
                break;
            default:
                throw new GeaflowException("Unsupported job type: {}", jobType);
        }

        job.setInstanceId(view.getInstanceId());
        job.setType(jobType);
        //TODO job.setSla(entity.getSlaId());
        return job;
    }

}
