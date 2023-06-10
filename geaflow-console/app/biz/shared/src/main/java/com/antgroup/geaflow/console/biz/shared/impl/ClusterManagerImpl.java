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

import com.antgroup.geaflow.console.biz.shared.ClusterManager;
import com.antgroup.geaflow.console.biz.shared.convert.ClusterViewConverter;
import com.antgroup.geaflow.console.biz.shared.convert.NameViewConverter;
import com.antgroup.geaflow.console.biz.shared.view.ClusterView;
import com.antgroup.geaflow.console.common.dal.model.ClusterSearch;
import com.antgroup.geaflow.console.common.util.ListUtil;
import com.antgroup.geaflow.console.common.util.exception.GeaflowIllegalException;
import com.antgroup.geaflow.console.core.model.cluster.GeaflowCluster;
import com.antgroup.geaflow.console.core.service.ClusterService;
import com.antgroup.geaflow.console.core.service.NameService;
import java.util.List;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class ClusterManagerImpl extends NameManagerImpl<GeaflowCluster, ClusterView, ClusterSearch> implements
    ClusterManager {

    @Autowired
    private ClusterService clusterService;

    @Autowired
    private ClusterViewConverter clusterViewConverter;

    @Override
    protected NameService<GeaflowCluster, ?, ClusterSearch> getService() {
        return clusterService;
    }

    @Override
    protected NameViewConverter<GeaflowCluster, ClusterView> getConverter() {
        return clusterViewConverter;
    }

    @Override
    protected List<GeaflowCluster> parse(List<ClusterView> views) {
        return ListUtil.convert(views, clusterViewConverter::convert);
    }

    @Override
    public String create(ClusterView view) {
        if (clusterService.existName(view.getName())) {
            throw new GeaflowIllegalException("Cluster name {} exists", view.getName());
        }

        return super.create(view);
    }
}
