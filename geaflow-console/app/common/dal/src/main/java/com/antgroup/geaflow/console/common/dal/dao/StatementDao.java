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

package com.antgroup.geaflow.console.common.dal.dao;

import com.antgroup.geaflow.console.common.dal.entity.StatementEntity;
import com.antgroup.geaflow.console.common.dal.mapper.StatementMapper;
import com.antgroup.geaflow.console.common.dal.model.StatementSearch;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import java.util.List;
import org.springframework.stereotype.Repository;

@Repository
public class StatementDao extends TenantLevelDao<StatementMapper, StatementEntity>
    implements IdDao<StatementEntity, StatementSearch> {

    @Override
    public void configSearch(LambdaQueryWrapper<StatementEntity> wrapper, StatementSearch search) {
        wrapper.eq(search.getJobId() != null, StatementEntity::getJobId, search.getJobId())
            .eq(search.getStatus() != null, StatementEntity::getStatus, search.getStatus());
    }

    public boolean dropByJobIds(List<String> jobIds) {
        return lambdaUpdate().in(StatementEntity::getJobId, jobIds).remove();
    }
}
