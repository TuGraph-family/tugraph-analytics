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

package com.antgroup.geaflow.console.biz.shared.impl;

import static com.antgroup.geaflow.console.core.service.RemoteFileService.JAR_FILE_SUFFIX;

import com.antgroup.geaflow.console.biz.shared.JobManager;
import com.antgroup.geaflow.console.biz.shared.RemoteFileManager;
import com.antgroup.geaflow.console.biz.shared.TaskManager;
import com.antgroup.geaflow.console.biz.shared.convert.IdViewConverter;
import com.antgroup.geaflow.console.biz.shared.convert.JobViewConverter;
import com.antgroup.geaflow.console.biz.shared.view.IdView;
import com.antgroup.geaflow.console.biz.shared.view.JobView;
import com.antgroup.geaflow.console.biz.shared.view.RemoteFileView;
import com.antgroup.geaflow.console.biz.shared.view.StructView;
import com.antgroup.geaflow.console.common.dal.entity.JobEntity;
import com.antgroup.geaflow.console.common.dal.model.JobSearch;
import com.antgroup.geaflow.console.common.util.ListUtil;
import com.antgroup.geaflow.console.common.util.context.ContextHolder;
import com.antgroup.geaflow.console.common.util.exception.GeaflowException;
import com.antgroup.geaflow.console.common.util.exception.GeaflowIllegalException;
import com.antgroup.geaflow.console.common.util.type.GeaflowJobType;
import com.antgroup.geaflow.console.common.util.type.GeaflowResourceType;
import com.antgroup.geaflow.console.common.util.type.GeaflowStructType;
import com.antgroup.geaflow.console.common.util.type.GeaflowTaskType;
import com.antgroup.geaflow.console.core.model.data.GeaflowFunction;
import com.antgroup.geaflow.console.core.model.data.GeaflowGraph;
import com.antgroup.geaflow.console.core.model.data.GeaflowStruct;
import com.antgroup.geaflow.console.core.model.file.GeaflowRemoteFile;
import com.antgroup.geaflow.console.core.model.job.GeaflowJob;
import com.antgroup.geaflow.console.core.service.AuthorizationService;
import com.antgroup.geaflow.console.core.service.DataService;
import com.antgroup.geaflow.console.core.service.IdService;
import com.antgroup.geaflow.console.core.service.JobService;
import com.antgroup.geaflow.console.core.service.ReleaseService;
import com.antgroup.geaflow.console.core.service.RemoteFileService;
import com.antgroup.geaflow.console.core.service.TaskService;
import com.antgroup.geaflow.console.core.service.file.RemoteFileStorage;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.multipart.MultipartFile;

@Service
@Slf4j
public class JobManagerImpl extends IdManagerImpl<GeaflowJob, JobView, JobSearch> implements JobManager {

    @Autowired
    private JobService jobService;

    @Autowired
    private JobViewConverter jobViewConverter;

    @Autowired
    private ReleaseService releaseService;

    @Autowired
    private TaskManager taskManager;

    @Autowired
    private TaskService taskService;

    @Autowired
    private RemoteFileManager remoteFileManager;

    @Autowired
    private AuthorizationService authorizationService;

    @Autowired
    private RemoteFileService remoteFileService;

    @Override
    public IdViewConverter<GeaflowJob, JobView> getConverter() {
        return jobViewConverter;
    }

    @Override
    public IdService<GeaflowJob, JobEntity, JobSearch> getService() {
        return jobService;
    }

    @Override
    public List<GeaflowJob> parse(List<JobView> views) {
        return views.stream().map(v -> {
            GeaflowJobType type = v.getType();
            Preconditions.checkNotNull(type, "job Type is null");

            switch (type) {
                case INTEGRATE:
                    // get graphs and tables
                    List<GeaflowStruct> structs = getResource(v.getStructs());
                    List<String> graphIds = ListUtil.convert(v.getGraphs(), IdView::getId);
                    List<GeaflowGraph> graphs = jobService.getResourceService(GeaflowResourceType.GRAPH).get(graphIds);
                    return jobViewConverter.convert(v, structs, graphs, null, null);
                case PROCESS:
                    // get functions
                    List<String> functionIds = ListUtil.convert(v.getFunctions(), IdView::getId);
                    List<GeaflowFunction> functions = jobService.getResourceService(GeaflowResourceType.FUNCTION).get(functionIds);
                    return jobViewConverter.convert(v, null, null, functions, null);
                case CUSTOM:
                    GeaflowRemoteFile remoteFile = Optional.ofNullable(v.getJarPackage())
                        .map(e -> remoteFileService.get(e.getId())).orElse(null);
                    return jobViewConverter.convert(v, null, null, null, remoteFile);
                default:
                    throw new GeaflowException("Unsupported job Type: ", v.getType());
            }
        }).collect(Collectors.toList());
    }

    private List<GeaflowStruct> getResource(List<StructView> views) {
        if (CollectionUtils.isEmpty(views)) {
            return new ArrayList<>();
        }
        // group by the structType
        Map<GeaflowStructType, List<StructView>> group = views.stream()
            .collect(Collectors.groupingBy(StructView::getType));
        List<GeaflowStruct> res = new ArrayList<>();
        // use services according to the group
        for (Entry<GeaflowStructType, List<StructView>> entry : group.entrySet()) {
            DataService dataService = jobService.getResourceService(GeaflowResourceType.valueOf(entry.getKey().name()));
            List<String> ids = ListUtil.convert(entry.getValue(), IdView::getId);
            res.addAll(dataService.get(ids));
        }
        return res;
    }


    @Override
    @Transactional
    public boolean drop(List<String> jobIds) {
        List<String> taskIds = taskService.getIdsByJob(jobIds);
        taskManager.drop(taskIds);
        releaseService.dropByJobIds(jobIds);
        jobService.dropResources(jobIds);
        authorizationService.dropByResources(jobIds, GeaflowResourceType.JOB);

        try {
            Map<String, String> jarIds = jobService.getJarIds(jobIds);
            for (String jobId : jobIds) {
                remoteFileManager.deleteJobJar(jarIds.get(jobId), jobId);
            }
        } catch (Exception e) {
            log.info(e.getMessage());
        }

        return super.drop(jobIds);
    }


    private String createApiJob(JobView jobView, MultipartFile jarFile, String fileId) {
        String jobName = jobView.getName();
        if (StringUtils.isBlank(jobName)) {
            throw new GeaflowIllegalException("Invalid function name");
        }

        if (jobService.existName(jobName)) {
            throw new GeaflowIllegalException("Job name {} exists", jobName);
        }

        Preconditions.checkNotNull(jobView.getEntryClass(), "Hla job needs entryClass");
        if (jarFile != null) {
            jobView.setJarPackage(createRemoteFile(jarFile));
        } else if (fileId != null) {
            // bind a jar file if jarId is not null
            if (!remoteFileService.exist(fileId)) {
                throw new GeaflowIllegalException("File {} does not exist", fileId);
            }
            RemoteFileView remoteFileView = new RemoteFileView();
            remoteFileView.setId(fileId);
            jobView.setJarPackage(remoteFileView);
        }

        // job package could be null
        return super.create(jobView);
    }


    private boolean updateApiJob(String jobId, JobView updateView, MultipartFile jarFile, String fileId) {
        Preconditions.checkNotNull(updateView.getEntryClass(), "Hla job needs entryClass");
        if (jarFile != null) {
            updateView.setJarPackage(createRemoteFile(jarFile));

            // try to delete old file
            GeaflowJob job = jobService.get(jobId);
            String oldJarId = job.getJarPackage().getId();
            try {
                remoteFileManager.deleteJobJar(oldJarId, jobId);
            } catch (Exception e) {
                log.info("delete job jar fail, jobName: {}, jarId: {}", job.getName(), oldJarId);
            }

        } else if (fileId != null) {
            // bind a jar file if jarId is not null
            if (!remoteFileService.exist(fileId)) {
                throw new GeaflowIllegalException("File {} does not exist", fileId);
            }
            RemoteFileView remoteFileView = new RemoteFileView();
            remoteFileView.setId(fileId);
            updateView.setJarPackage(remoteFileView);
        }

        return updateById(jobId, updateView);
    }

    private RemoteFileView createRemoteFile(MultipartFile jarFile) {
        if (!StringUtils.endsWith(jarFile.getOriginalFilename(), JAR_FILE_SUFFIX)) {
            throw new GeaflowIllegalException("Invalid jar file");
        }

        String fileName = jarFile.getOriginalFilename();
        if (remoteFileService.existName(fileName)) {
            throw new GeaflowException("FileName {} exists", fileName);
        }

        String path = RemoteFileStorage.getUserFilePath(ContextHolder.get().getUserId(), fileName);

        RemoteFileView remoteFileView = new RemoteFileView();
        remoteFileView.setName(fileName);
        remoteFileView.setPath(path);
        remoteFileManager.create(remoteFileView, jarFile);

        return remoteFileView;
    }

    @Override
    @Transactional
    public String create(JobView jobView, MultipartFile jarFile, String fileId) {
        Preconditions.checkNotNull(jobView.getType(), "Job type is null");
        return jobView.getType().getTaskType() == GeaflowTaskType.API ? createApiJob(jobView, jarFile, fileId) :
               super.create(jobView);
    }

    @Override
    public boolean update(String jobId, JobView jobView, MultipartFile jarFile, String fileId) {
        Preconditions.checkNotNull(jobView.getType(), "Job type is null");
        return jobView.getType().getTaskType() == GeaflowTaskType.API ? updateApiJob(jobId, jobView, jarFile, fileId) :
               super.updateById(jobId, jobView);
    }
}
