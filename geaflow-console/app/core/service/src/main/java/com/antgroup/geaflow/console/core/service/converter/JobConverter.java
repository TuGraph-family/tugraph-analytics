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

package com.antgroup.geaflow.console.core.service.converter;

import com.alibaba.fastjson.JSON;
import com.antgroup.geaflow.console.common.dal.entity.JobEntity;
import com.antgroup.geaflow.console.common.util.exception.GeaflowException;
import com.antgroup.geaflow.console.common.util.type.GeaflowJobType;
import com.antgroup.geaflow.console.core.model.GeaflowId;
import com.antgroup.geaflow.console.core.model.code.GeaflowCode;
import com.antgroup.geaflow.console.core.model.data.GeaflowFunction;
import com.antgroup.geaflow.console.core.model.data.GeaflowGraph;
import com.antgroup.geaflow.console.core.model.data.GeaflowStruct;
import com.antgroup.geaflow.console.core.model.file.GeaflowRemoteFile;
import com.antgroup.geaflow.console.core.model.job.GeaflowCustomJob;
import com.antgroup.geaflow.console.core.model.job.GeaflowIntegrateJob;
import com.antgroup.geaflow.console.core.model.job.GeaflowJob;
import com.antgroup.geaflow.console.core.model.job.GeaflowProcessJob;
import com.antgroup.geaflow.console.core.model.job.GeaflowServeJob;
import com.antgroup.geaflow.console.core.model.job.GeaflowTransferJob.StructMapping;
import com.antgroup.geaflow.console.core.model.plugin.GeaflowPlugin;
import java.util.List;
import java.util.Optional;
import org.springframework.stereotype.Component;

@Component
public class JobConverter extends NameConverter<GeaflowJob, JobEntity> {

    @Override
    protected JobEntity modelToEntity(GeaflowJob model) {
        GeaflowJobType jobType = model.getType();
        JobEntity entity = super.modelToEntity(model);
        entity.setType(jobType);
        entity.setUserCode(Optional.ofNullable(model.getUserCode()).map(GeaflowCode::getText).orElse(null));
        entity.setStructMappings(Optional.ofNullable(model.getStructMappings()).map(JSON::toJSONString).orElse(null));
        entity.setInstanceId(model.getInstanceId());
        entity.setJarPackageId(Optional.ofNullable(model.getJarPackage()).map(GeaflowId::getId).orElse(null));
        entity.setEntryClass(model.getEntryClass());
        return entity;
    }


    public GeaflowJob convert(JobEntity entity, List<GeaflowStruct> structs, List<GeaflowGraph> graphs, List<GeaflowFunction> functions,
                              List<GeaflowPlugin> plugins, GeaflowRemoteFile jarPackage) {
        GeaflowJobType jobType = entity.getType();
        GeaflowJob job;
        switch (jobType) {
            case INTEGRATE:
                GeaflowIntegrateJob integrateJob = (GeaflowIntegrateJob) super.entityToModel(entity, GeaflowIntegrateJob.class);
                List<StructMapping> structMappings = JSON.parseArray(entity.getStructMappings(), StructMapping.class);
                integrateJob.setStructMappings(structMappings);
                integrateJob.setGraph(graphs);
                integrateJob.setStructs(structs);
                integrateJob.setUserCode(entity.getUserCode());
                job = integrateJob;
                break;
            case PROCESS:
                GeaflowProcessJob processJob = (GeaflowProcessJob) super.entityToModel(entity, GeaflowProcessJob.class);
                processJob.setUserCode(entity.getUserCode());
                processJob.setFunctions(functions);
                processJob.setPlugins(plugins);
                processJob.setStructs(structs);
                processJob.setGraph(graphs);
                job = processJob;
                break;
            case CUSTOM:
                GeaflowCustomJob customJob = (GeaflowCustomJob) super.entityToModel(entity, GeaflowCustomJob.class);
                customJob.setEntryClass(entity.getEntryClass());
                customJob.setJarPackage(jarPackage);
                job = customJob;
                break;
            case SERVE:
                GeaflowServeJob serveJob = (GeaflowServeJob) super.entityToModel(entity, GeaflowServeJob.class);
                serveJob.setEntryClass(entity.getEntryClass());
                serveJob.setGraph(graphs);
                job = serveJob;
                break;
            default:
                throw new GeaflowException("Unsupported job type: {}", jobType);
        }

        job.setType(entity.getType());
        job.setInstanceId(entity.getInstanceId());
        //TODO job.setSla(entity.getSlaId());
        return job;
    }

}
