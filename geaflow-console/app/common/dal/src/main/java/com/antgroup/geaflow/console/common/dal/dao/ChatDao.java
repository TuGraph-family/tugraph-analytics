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

import com.antgroup.geaflow.console.common.dal.entity.ChatEntity;
import com.antgroup.geaflow.console.common.dal.mapper.ChatMapper;
import com.antgroup.geaflow.console.common.dal.model.ChatSearch;
import com.antgroup.geaflow.console.common.util.context.ContextHolder;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import org.springframework.stereotype.Repository;

@Repository
public class ChatDao extends TenantLevelDao<ChatMapper, ChatEntity> implements IdDao<ChatEntity, ChatSearch> {

    public boolean dropByJobId(String jobId) {
        String userId = ContextHolder.get().getUserId();
        return lambdaUpdate()
            .eq(ChatEntity::getCreatorId, userId)
            .eq(ChatEntity::getJobId, jobId)
            .remove();
    }


    @Override
    public void configSearch(LambdaQueryWrapper<ChatEntity> wrapper, ChatSearch search) {
        wrapper.eq(search.getJobId() != null, ChatEntity::getJobId, search.getJobId());
        wrapper.eq(search.getModelId() != null, ChatEntity::getModelId, search.getModelId());
    }
}
