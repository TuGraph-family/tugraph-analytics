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

import com.antgroup.geaflow.console.common.dal.dao.DataDao;
import com.antgroup.geaflow.console.common.dal.dao.FunctionDao;
import com.antgroup.geaflow.console.common.dal.entity.FunctionEntity;
import com.antgroup.geaflow.console.common.dal.model.FunctionSearch;
import com.antgroup.geaflow.console.common.util.ListUtil;
import com.antgroup.geaflow.console.core.model.GeaflowId;
import com.antgroup.geaflow.console.core.model.data.GeaflowFunction;
import com.antgroup.geaflow.console.core.model.file.GeaflowRemoteFile;
import com.antgroup.geaflow.console.core.service.converter.DataConverter;
import com.antgroup.geaflow.console.core.service.converter.FunctionConverter;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class FunctionService extends DataService<GeaflowFunction, FunctionEntity, FunctionSearch> {

    @Autowired
    private FunctionDao functionDao;

    @Autowired
    private RemoteFileService remoteFileService;

    @Autowired
    private FunctionConverter functionConverter;

    @Override
    protected DataDao<FunctionEntity, FunctionSearch> getDao() {
        return functionDao;
    }

    @Override
    protected DataConverter<GeaflowFunction, FunctionEntity> getConverter() {
        return functionConverter;
    }

    @Override
    protected List<GeaflowFunction> parse(List<FunctionEntity> functionEntities) {
        // get jar packages
        List<String> packageIds = functionEntities.stream().map(FunctionEntity::getJarPackageId)
            .collect(Collectors.toList());
        List<GeaflowRemoteFile> jarPackages = remoteFileService.get(packageIds);

        Map<String, GeaflowRemoteFile> map = ListUtil.toMap(jarPackages, GeaflowId::getId);

        return functionEntities.stream().map(e -> {
            GeaflowRemoteFile jarPackage = map.get(e.getJarPackageId());
            return functionConverter.convert(e, jarPackage);
        }).collect(Collectors.toList());
    }

    public long getFileRefCount(String fileId, String excludeFunctionId) {
        return functionDao.getFileRefCount(fileId, excludeFunctionId);
    }

}

