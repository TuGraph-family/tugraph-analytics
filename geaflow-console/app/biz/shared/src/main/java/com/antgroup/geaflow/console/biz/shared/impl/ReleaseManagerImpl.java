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


import com.antgroup.geaflow.console.biz.shared.ReleaseManager;
import com.antgroup.geaflow.console.biz.shared.convert.IdViewConverter;
import com.antgroup.geaflow.console.biz.shared.convert.ReleaseUpdateViewConverter;
import com.antgroup.geaflow.console.biz.shared.convert.ReleaseViewConverter;
import com.antgroup.geaflow.console.biz.shared.view.ReleaseUpdateView;
import com.antgroup.geaflow.console.biz.shared.view.ReleaseView;
import com.antgroup.geaflow.console.common.dal.model.ReleaseSearch;
import com.antgroup.geaflow.console.common.util.Fmt;
import com.antgroup.geaflow.console.common.util.exception.GeaflowException;
import com.antgroup.geaflow.console.common.util.type.GeaflowOperationType;
import com.antgroup.geaflow.console.common.util.type.GeaflowTaskStatus;
import com.antgroup.geaflow.console.core.model.cluster.GeaflowCluster;
import com.antgroup.geaflow.console.core.model.job.GeaflowJob;
import com.antgroup.geaflow.console.core.model.release.GeaflowRelease;
import com.antgroup.geaflow.console.core.model.release.ReleaseUpdate;
import com.antgroup.geaflow.console.core.model.runtime.GeaflowAudit;
import com.antgroup.geaflow.console.core.model.task.GeaflowTask;
import com.antgroup.geaflow.console.core.model.version.GeaflowVersion;
import com.antgroup.geaflow.console.core.service.AuditService;
import com.antgroup.geaflow.console.core.service.ClusterService;
import com.antgroup.geaflow.console.core.service.IdService;
import com.antgroup.geaflow.console.core.service.JobService;
import com.antgroup.geaflow.console.core.service.ReleaseService;
import com.antgroup.geaflow.console.core.service.TaskService;
import com.antgroup.geaflow.console.core.service.VersionService;
import com.antgroup.geaflow.console.core.service.release.GeaflowReleaseBuilder;
import java.util.List;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class ReleaseManagerImpl extends IdManagerImpl<GeaflowRelease, ReleaseView, ReleaseSearch> implements ReleaseManager {

    @Autowired
    private JobService jobService;

    @Autowired
    private ReleaseService releaseService;

    @Autowired
    private ReleaseUpdateViewConverter releaseUpdateViewConverter;

    @Autowired
    private TaskService taskService;

    @Autowired
    private VersionService versionService;

    @Autowired
    private ClusterService clusterService;

    @Autowired
    private ReleaseViewConverter releaseViewConverter;

    @Autowired
    private AuditService auditService;

    @Override
    protected IdService<GeaflowRelease, ?, ReleaseSearch> getService() {
        return releaseService;
    }

    @Override
    protected IdViewConverter<GeaflowRelease, ReleaseView> getConverter() {
        return releaseViewConverter;
    }

    @Override
    protected List<GeaflowRelease> parse(List<ReleaseView> views) {
        throw new UnsupportedOperationException("Release can't be converted from view");
    }

    @Override
    @Transactional(rollbackFor = Exception.class)
    public String publish(String jobId) {
        GeaflowJob job = jobService.get(jobId);

        GeaflowRelease release = GeaflowReleaseBuilder.build(job);

        boolean newRelease = release.getId() == null;
        String releaseId;
        // handle release
        if (newRelease) {
            // the task status is not created, create a new release, version+1
            releaseId = releaseService.create(release);
        } else {
            // the task status is  created, update release, version unchanged
            releaseService.update(release);
            releaseId = release.getId();
        }

        // handle task
        if (newRelease) {
            String taskId;

            if (release.getReleaseVersion() == 1) {
                // create a task when first publishing
                taskId = taskService.createTask(release).get(0).getId();
            } else {
                // bind task with release for later publishing
                taskId = taskService.bindRelease(release);
            }

            if (taskId != null) {
                String detail = Fmt.as("Publish version {}", release.getReleaseVersion());
                auditService.create(new GeaflowAudit(taskId, GeaflowOperationType.PUBLISH, detail));
            }
        }

        return releaseId;
    }


    @Override
    public boolean updateRelease(String jobId, ReleaseUpdateView view) {
        GeaflowTask task = taskService.getByJobId(jobId);

        if (task.getStatus() != GeaflowTaskStatus.CREATED) {
            throw new GeaflowException("Only created status can be updated");
        }

        GeaflowVersion version = versionService.getByName(view.getVersionName());
        GeaflowCluster cluster = clusterService.getByName(view.getClusterName());
        ReleaseUpdate releaseUpdate = releaseUpdateViewConverter.converter(view, version, cluster);

        GeaflowRelease newRelease = GeaflowReleaseBuilder.update(task.getRelease(), releaseUpdate);

        return releaseService.update(newRelease);
    }

}
