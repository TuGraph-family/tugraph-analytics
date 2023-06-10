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

import com.antgroup.geaflow.console.common.dal.entity.InstanceEntity;
import com.antgroup.geaflow.console.common.dal.entity.ResourceCount;
import com.antgroup.geaflow.console.common.dal.mapper.InstanceMapper;
import com.antgroup.geaflow.console.common.dal.model.InstanceSearch;
import com.antgroup.geaflow.console.common.util.exception.GeaflowException;
import java.util.List;
import org.apache.commons.collections.CollectionUtils;
import org.springframework.stereotype.Repository;

@Repository
public class InstanceDao extends TenantLevelDao<InstanceMapper, InstanceEntity> implements NameDao<InstanceEntity, InstanceSearch> {

    public List<InstanceEntity> search() {
        return lambdaQuery().list();
    }


    public List<ResourceCount> getResourceCount(String instanceId, List<String> names) {
        if (instanceId == null || CollectionUtils.isEmpty(names)) {
            throw new GeaflowException("Empty instance or names");
        }

        return getBaseMapper().getResourceCount(instanceId, names);
    }
}
