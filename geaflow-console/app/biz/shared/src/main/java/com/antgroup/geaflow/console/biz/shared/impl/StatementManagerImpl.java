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

import com.antgroup.geaflow.console.biz.shared.StatementManager;
import com.antgroup.geaflow.console.biz.shared.convert.IdViewConverter;
import com.antgroup.geaflow.console.biz.shared.convert.StatementViewConverter;
import com.antgroup.geaflow.console.biz.shared.view.StatementView;
import com.antgroup.geaflow.console.common.dal.entity.StatementEntity;
import com.antgroup.geaflow.console.common.dal.model.StatementSearch;
import com.antgroup.geaflow.console.common.util.ListUtil;
import com.antgroup.geaflow.console.core.model.statement.GeaflowStatement;
import com.antgroup.geaflow.console.core.service.IdService;
import com.antgroup.geaflow.console.core.service.StatementService;
import java.util.Collections;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class StatementManagerImpl extends IdManagerImpl<GeaflowStatement, StatementView, StatementSearch>
    implements StatementManager {

    @Autowired
    private StatementService statementService;

    @Autowired
    private StatementViewConverter statementViewConverter;

    @Override
    public IdViewConverter<GeaflowStatement, StatementView> getConverter() {
        return statementViewConverter;
    }

    @Override
    protected List<GeaflowStatement> parse(List<StatementView> views) {
        return ListUtil.convert(views, v -> statementViewConverter.convert(v));
    }

    @Override
    public IdService<GeaflowStatement, StatementEntity, StatementSearch> getService() {
        return statementService;
    }

    @Override
    public boolean dropByJobId(String jobId) {
        return statementService.dropByJobIds(Collections.singletonList(jobId));
    }
}
