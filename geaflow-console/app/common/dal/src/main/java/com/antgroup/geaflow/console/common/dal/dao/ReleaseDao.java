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

import com.antgroup.geaflow.console.common.dal.entity.ReleaseEntity;
import com.antgroup.geaflow.console.common.dal.mapper.ReleaseMapper;
import com.antgroup.geaflow.console.common.dal.model.ReleaseSearch;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import java.util.List;
import org.springframework.stereotype.Repository;

@Repository
public class ReleaseDao extends TenantLevelDao<ReleaseMapper, ReleaseEntity> implements IdDao<ReleaseEntity, ReleaseSearch> {

    public ReleaseEntity getLatestRelease(String jobId) {
        return lambdaQuery().eq(ReleaseEntity::getJobId, jobId).orderByDesc(ReleaseEntity::getVersion).last("limit 1").one();
    }

    @Override
    public void configSearch(LambdaQueryWrapper<ReleaseEntity> wrapper, ReleaseSearch search) {
        wrapper.eq(search.getVersionId() != null, ReleaseEntity::getVersionId, search.getVersionId())
            .eq(search.getClusterId() != null, ReleaseEntity::getClusterId, search.getClusterId())
            .eq(search.getJobId() != null, ReleaseEntity::getJobId, search.getJobId());

    }

    public void dropByJobIds(List<String> ids) {
        lambdaUpdate().in(ReleaseEntity::getJobId, ids).remove();
    }

    public List<ReleaseEntity> getByJobIds(List<String> ids) {
        return lambdaQuery().in(ReleaseEntity::getJobId, ids).list();
    }


}
