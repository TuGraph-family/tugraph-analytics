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

import com.antgroup.geaflow.console.biz.shared.VertexManager;
import com.antgroup.geaflow.console.biz.shared.convert.DataViewConverter;
import com.antgroup.geaflow.console.biz.shared.convert.FieldViewConverter;
import com.antgroup.geaflow.console.biz.shared.convert.VertexViewConverter;
import com.antgroup.geaflow.console.biz.shared.view.VertexView;
import com.antgroup.geaflow.console.common.dal.entity.VertexEntity;
import com.antgroup.geaflow.console.common.dal.model.VertexSearch;
import com.antgroup.geaflow.console.common.util.ListUtil;
import com.antgroup.geaflow.console.core.model.data.GeaflowField;
import com.antgroup.geaflow.console.core.model.data.GeaflowVertex;
import com.antgroup.geaflow.console.core.service.DataService;
import com.antgroup.geaflow.console.core.service.VertexService;
import java.util.List;
import java.util.stream.Collectors;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class VertexManagerImpl extends DataManagerImpl<GeaflowVertex, VertexView, VertexSearch> implements
    VertexManager {

    @Autowired
    private VertexService vertexService;

    @Autowired
    private VertexViewConverter vertexViewConverter;

    @Autowired
    private FieldViewConverter fieldViewConverter;

    @Override
    public DataViewConverter<GeaflowVertex, VertexView> getConverter() {
        return vertexViewConverter;
    }

    @Override
    public DataService<GeaflowVertex, VertexEntity, VertexSearch> getService() {
        return vertexService;
    }

    @Override
    protected List<GeaflowVertex> parse(List<VertexView> views) {
        return views.stream().map(e -> {
            List<GeaflowField> fields = ListUtil.convert(e.getFields(), fieldViewConverter::convert);
            return vertexViewConverter.converter(e, fields);
        }).collect(Collectors.toList());
    }

    @Override
    public List<GeaflowVertex> getVerticesByGraphId(String graphId) {
        return vertexService.getVerticesByGraphId(graphId);
    }

    @Override
    public List<GeaflowVertex> getVerticesByEdgeId(String edgeId) {
        return vertexService.getVerticesByEdgeId(edgeId);
    }

}
