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

import com.antgroup.geaflow.console.common.dal.entity.RemoteFileEntity;
import com.antgroup.geaflow.console.common.dal.mapper.RemoteFileMapper;
import com.antgroup.geaflow.console.common.dal.model.RemoteFileSearch;
import com.antgroup.geaflow.console.common.util.context.ContextHolder;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import org.springframework.stereotype.Repository;

@Repository
public class RemoteFileDao extends UserLevelDao<RemoteFileMapper, RemoteFileEntity> implements
    NameDao<RemoteFileEntity, RemoteFileSearch> {

    public void updateMd5(String id, String md5) {
        lambdaUpdate().set(RemoteFileEntity::getMd5, md5).eq(RemoteFileEntity::getId, id).update();
    }

    public void updateUrl(String id, String url) {
        lambdaUpdate().set(RemoteFileEntity::getUrl, url).eq(RemoteFileEntity::getId, id).update();
    }

    @Override
    public void configSearch(LambdaQueryWrapper<RemoteFileEntity> wrapper, RemoteFileSearch search) {
        boolean systemSession = ContextHolder.get().isSystemSession();
        String userId = ContextHolder.get().getUserId();
        wrapper.eq(search.getType() != null, RemoteFileEntity::getType, search.getType())
            .eq(!systemSession, RemoteFileEntity::getCreatorId, userId)
            .eq(RemoteFileEntity::isSystem, systemSession);
    }
}
