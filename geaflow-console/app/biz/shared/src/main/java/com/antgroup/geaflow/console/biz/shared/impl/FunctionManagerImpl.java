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

import com.antgroup.geaflow.console.biz.shared.FunctionManager;
import com.antgroup.geaflow.console.biz.shared.convert.DataViewConverter;
import com.antgroup.geaflow.console.biz.shared.convert.FunctionViewConverter;
import com.antgroup.geaflow.console.biz.shared.view.FunctionView;
import com.antgroup.geaflow.console.common.dal.entity.FunctionEntity;
import com.antgroup.geaflow.console.common.dal.model.FunctionSearch;
import com.antgroup.geaflow.console.core.model.GeaflowId;
import com.antgroup.geaflow.console.core.model.data.GeaflowFunction;
import com.antgroup.geaflow.console.core.model.file.GeaflowJarPackage;
import com.antgroup.geaflow.console.core.model.file.GeaflowRemoteFile;
import com.antgroup.geaflow.console.core.service.DataService;
import com.antgroup.geaflow.console.core.service.FunctionService;
import com.antgroup.geaflow.console.core.service.RemoteFileService;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class FunctionManagerImpl extends DataManagerImpl<GeaflowFunction, FunctionView, FunctionSearch> implements
    FunctionManager {

    @Autowired
    private FunctionViewConverter functionViewConverter;

    @Autowired
    private FunctionService functionService;

    @Autowired
    private RemoteFileService remoteFileService;

    @Override
    protected DataViewConverter<GeaflowFunction, FunctionView> getConverter() {
        return functionViewConverter;
    }

    @Override
    protected DataService<GeaflowFunction, FunctionEntity, FunctionSearch> getService() {
        return functionService;
    }

    @Override
    protected List<GeaflowFunction> parse(List<FunctionView> views) {
        List<String> packageIds = views.stream().map(e -> e.getJarPackage().getId()).collect(Collectors.toList());
        List<GeaflowRemoteFile> jarPackages = remoteFileService.get(packageIds);
        Map<String, GeaflowJarPackage> map = jarPackages.stream()
            .collect(Collectors.toMap(GeaflowId::getId, e -> (GeaflowJarPackage) e));

        return views.stream().map(e -> {
            GeaflowJarPackage jarPackage = map.get(e.getJarPackage().getId());
            return functionViewConverter.convert(e, jarPackage);
        }).collect(Collectors.toList());
    }
}
