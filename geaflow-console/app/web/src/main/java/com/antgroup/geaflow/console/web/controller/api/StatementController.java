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

package com.antgroup.geaflow.console.web.controller.api;

import static com.antgroup.geaflow.console.common.dal.dao.IdDao.CREATE_TIME_FIELD_NAME;

import com.antgroup.geaflow.console.biz.shared.StatementManager;
import com.antgroup.geaflow.console.biz.shared.view.StatementView;
import com.antgroup.geaflow.console.common.dal.model.PageList;
import com.antgroup.geaflow.console.common.dal.model.PageSearch.SortOrder;
import com.antgroup.geaflow.console.common.dal.model.StatementSearch;
import com.antgroup.geaflow.console.web.api.GeaflowApiResponse;
import org.apache.commons.lang.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/statements")
public class StatementController {

    @Autowired
    private StatementManager statementManager;

    @GetMapping
    public GeaflowApiResponse<PageList<StatementView>> searchStatement(StatementSearch search) {
        if (StringUtils.isEmpty(search.getSort())) {
            search.setOrder(SortOrder.DESC, CREATE_TIME_FIELD_NAME);
        }
        return GeaflowApiResponse.success(statementManager.search(search));
    }

    @GetMapping("/{statementId}")
    public GeaflowApiResponse<StatementView> getStatement(@PathVariable String statementId) {
        return GeaflowApiResponse.success(statementManager.get(statementId));
    }

    @PostMapping
    public GeaflowApiResponse<String> createStatement(StatementView statementView) {
        return GeaflowApiResponse.success(statementManager.create(statementView));
    }


    @DeleteMapping("/{statementId}")
    public GeaflowApiResponse<Boolean> deleteStatement(@PathVariable String statementId) {
        return GeaflowApiResponse.success(statementManager.drop(statementId));
    }

    @DeleteMapping("/jobs/{jobId}")
    public GeaflowApiResponse<Boolean> deleteJobStatement(@PathVariable String jobId) {
        return GeaflowApiResponse.success(statementManager.dropByJobId(jobId));
    }

}
