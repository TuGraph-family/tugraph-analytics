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

import com.antgroup.geaflow.console.common.dal.entity.ClusterEntity;
import com.antgroup.geaflow.console.common.dal.entity.IdEntity;
import com.antgroup.geaflow.console.common.dal.mapper.ClusterMapper;
import com.antgroup.geaflow.console.common.dal.model.ClusterSearch;
import org.springframework.stereotype.Repository;

@Repository
public class ClusterDao extends SystemLevelDao<ClusterMapper, ClusterEntity> implements NameDao<ClusterEntity, ClusterSearch> {

    public ClusterEntity getDefaultCluster() {
        return lambdaQuery().orderByDesc(IdEntity::getGmtModified).last("limit 1").one();
    }
}
