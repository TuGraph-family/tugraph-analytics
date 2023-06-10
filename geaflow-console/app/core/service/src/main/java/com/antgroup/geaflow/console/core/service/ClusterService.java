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

import com.antgroup.geaflow.console.common.dal.dao.ClusterDao;
import com.antgroup.geaflow.console.common.dal.dao.NameDao;
import com.antgroup.geaflow.console.common.dal.entity.ClusterEntity;
import com.antgroup.geaflow.console.common.dal.model.ClusterSearch;
import com.antgroup.geaflow.console.common.util.ListUtil;
import com.antgroup.geaflow.console.core.model.cluster.GeaflowCluster;
import com.antgroup.geaflow.console.core.service.converter.ClusterConverter;
import com.antgroup.geaflow.console.core.service.converter.NameConverter;
import java.util.List;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class ClusterService extends NameService<GeaflowCluster, ClusterEntity, ClusterSearch> {

    @Autowired
    private ClusterDao clusterDao;

    @Autowired
    private ClusterConverter clusterConverter;

    @Override
    protected NameDao<ClusterEntity, ClusterSearch> getDao() {
        return clusterDao;
    }

    @Override
    protected NameConverter<GeaflowCluster, ClusterEntity> getConverter() {
        return clusterConverter;
    }

    @Override
    protected List<GeaflowCluster> parse(List<ClusterEntity> clusterEntities) {
        return ListUtil.convert(clusterEntities, clusterConverter::convert);
    }

    public GeaflowCluster getDefaultCluster() {
        return parse(clusterDao.getDefaultCluster());
    }
}

