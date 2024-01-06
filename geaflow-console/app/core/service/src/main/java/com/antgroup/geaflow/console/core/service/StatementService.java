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
import com.antgroup.geaflow.console.common.dal.dao.StatementDao;
import com.antgroup.geaflow.console.common.dal.entity.StatementEntity;
import com.antgroup.geaflow.console.common.dal.model.StatementSearch;
import com.antgroup.geaflow.console.common.util.ListUtil;
import com.antgroup.geaflow.console.common.util.exception.GeaflowException;
import com.antgroup.geaflow.console.common.util.type.GeaflowStatementStatus;
import com.antgroup.geaflow.console.common.util.type.GeaflowTaskStatus;
import com.antgroup.geaflow.console.core.model.job.GeaflowJob;
import com.antgroup.geaflow.console.core.model.statement.GeaflowStatement;
import com.antgroup.geaflow.console.core.model.task.GeaflowTask;
import com.antgroup.geaflow.console.core.service.converter.IdConverter;
import com.antgroup.geaflow.console.core.service.converter.StatementConverter;
import com.antgroup.geaflow.console.core.service.statement.StatementSubmitter;
import com.google.common.base.Preconditions;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class StatementService extends IdService<GeaflowStatement, StatementEntity, StatementSearch> {

    @Autowired
    private StatementDao statementDao;

    @Autowired
    private StatementConverter statementConverter;

    @Autowired
    private StatementSubmitter statementSubmitter;

    @Autowired
    private TaskService taskService;

    @Override
    protected IdDao<StatementEntity, StatementSearch> getDao() {
        return statementDao;
    }

    @Override
    protected IdConverter<GeaflowStatement, StatementEntity> getConverter() {
        return statementConverter;
    }

    @Override
    protected List<GeaflowStatement> parse(List<StatementEntity> entities) {
        return ListUtil.convert(entities, e -> statementConverter.convert(e));
    }
    
    @Override
    public List<String> create(List<GeaflowStatement> models) {
        Map<String, GeaflowTask> taskMap = new HashMap<>();
        for (GeaflowStatement model : models) {
            String jobId = model.getJobId();

            GeaflowTask task = null;
            if (!taskMap.containsKey(jobId)) {
                task = taskService.getByJobId(jobId);
                Preconditions.checkNotNull(task, "Job %s task is null, please publish job", jobId);
                taskMap.put(jobId, task);
            } else {
                task = taskMap.get(jobId);
            }

            GeaflowJob job = task.getRelease().getJob();
            if (task.getStatus() != GeaflowTaskStatus.RUNNING) {
                throw new GeaflowException("Job {} task is not running", job.getName());
            }

            model.setStatus(GeaflowStatementStatus.RUNNING);
            model.setResult("Query is running, please wait or refresh the page");
        }

        List<String> ids = super.create(models);

        for (GeaflowStatement model : models) {
            statementSubmitter.asyncSubmitQuery(model, taskMap.get(model.getJobId()));
        }

        return ids;
    }

    public boolean dropByJobIds(List<String> jobIds) {
        return statementDao.dropByJobIds(jobIds);
    }
}
