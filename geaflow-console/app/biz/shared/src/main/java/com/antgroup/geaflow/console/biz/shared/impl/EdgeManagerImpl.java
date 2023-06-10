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

import com.antgroup.geaflow.console.biz.shared.EdgeManager;
import com.antgroup.geaflow.console.biz.shared.convert.DataViewConverter;
import com.antgroup.geaflow.console.biz.shared.convert.EdgeViewConverter;
import com.antgroup.geaflow.console.biz.shared.convert.FieldViewConverter;
import com.antgroup.geaflow.console.biz.shared.view.EdgeView;
import com.antgroup.geaflow.console.common.dal.entity.EdgeEntity;
import com.antgroup.geaflow.console.common.dal.model.EdgeSearch;
import com.antgroup.geaflow.console.common.util.ListUtil;
import com.antgroup.geaflow.console.core.model.data.GeaflowEdge;
import com.antgroup.geaflow.console.core.model.data.GeaflowField;
import com.antgroup.geaflow.console.core.service.DataService;
import com.antgroup.geaflow.console.core.service.EdgeService;
import java.util.List;
import java.util.stream.Collectors;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class EdgeManagerImpl extends DataManagerImpl<GeaflowEdge, EdgeView, EdgeSearch> implements EdgeManager {

    @Autowired
    private EdgeService edgeService;

    @Autowired
    private EdgeViewConverter edgeViewConverter;

    @Autowired
    private FieldViewConverter fieldViewConverter;

    @Override
    public DataViewConverter<GeaflowEdge, EdgeView> getConverter() {
        return edgeViewConverter;
    }

    @Override
    public DataService<GeaflowEdge, EdgeEntity, EdgeSearch> getService() {
        return edgeService;
    }


    @Override
    protected List<GeaflowEdge> parse(List<EdgeView> views) {
        return views.stream().map(e -> {
            List<GeaflowField> fields = ListUtil.convert(e.getFields(), fieldViewConverter::convert);
            return edgeViewConverter.converter(e, fields);
        }).collect(Collectors.toList());
    }

}
