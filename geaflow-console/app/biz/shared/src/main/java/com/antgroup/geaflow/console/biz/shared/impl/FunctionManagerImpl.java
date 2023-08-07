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

import static com.antgroup.geaflow.console.core.service.RemoteFileService.JAR_FILE_SUFFIX;

import com.antgroup.geaflow.console.biz.shared.FunctionManager;
import com.antgroup.geaflow.console.biz.shared.RemoteFileManager;
import com.antgroup.geaflow.console.biz.shared.convert.DataViewConverter;
import com.antgroup.geaflow.console.biz.shared.convert.FunctionViewConverter;
import com.antgroup.geaflow.console.biz.shared.view.FunctionView;
import com.antgroup.geaflow.console.biz.shared.view.RemoteFileView;
import com.antgroup.geaflow.console.common.dal.entity.FunctionEntity;
import com.antgroup.geaflow.console.common.dal.model.FunctionSearch;
import com.antgroup.geaflow.console.common.util.context.ContextHolder;
import com.antgroup.geaflow.console.common.util.exception.GeaflowException;
import com.antgroup.geaflow.console.common.util.exception.GeaflowIllegalException;
import com.antgroup.geaflow.console.core.model.GeaflowId;
import com.antgroup.geaflow.console.core.model.data.GeaflowFunction;
import com.antgroup.geaflow.console.core.model.file.GeaflowRemoteFile;
import com.antgroup.geaflow.console.core.service.DataService;
import com.antgroup.geaflow.console.core.service.FunctionService;
import com.antgroup.geaflow.console.core.service.JobService;
import com.antgroup.geaflow.console.core.service.RemoteFileService;
import com.antgroup.geaflow.console.core.service.file.RemoteFileStorage;
import com.google.common.base.Preconditions;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.multipart.MultipartFile;

@Service
@Slf4j
public class FunctionManagerImpl extends DataManagerImpl<GeaflowFunction, FunctionView, FunctionSearch> implements
    FunctionManager {

    @Autowired
    private FunctionViewConverter functionViewConverter;

    @Autowired
    private FunctionService functionService;

    @Autowired
    private JobService jobService;

    @Autowired
    private RemoteFileService remoteFileService;

    @Autowired
    private RemoteFileManager remoteFileManager;

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
        Map<String, GeaflowRemoteFile> map = jarPackages.stream()
            .collect(Collectors.toMap(GeaflowId::getId, e -> e));

        return views.stream().map(e -> {
            GeaflowRemoteFile jarPackage = map.get(e.getJarPackage().getId());
            return functionViewConverter.convert(e, jarPackage);
        }).collect(Collectors.toList());
    }

    @Override
    @Transactional
    public String createFunction(String instanceName, FunctionView functionView, MultipartFile functionFile, String fileId) {
        String functionName = functionView.getName();
        if (StringUtils.isBlank(functionName)) {
            throw new GeaflowIllegalException("Invalid function name");
        }

        if (functionService.existName(functionName)) {
            throw new GeaflowIllegalException("Function name {} exists", functionName);
        }

        Preconditions.checkNotNull(functionView.getEntryClass(), "Function needs entryClass");
        if (fileId == null) {
            Preconditions.checkNotNull(functionFile, "Invalid function file");
            functionView.setJarPackage(createRemoteFile(functionFile));
        } else {
            // bind a jar file if jarId is not null
            if (!remoteFileService.exist(fileId)) {
                throw new GeaflowIllegalException("File {} does not exist", fileId);
            }
            RemoteFileView remoteFileView = new RemoteFileView();
            remoteFileView.setId(fileId);
            functionView.setJarPackage(remoteFileView);
        }

        return super.create(instanceName, functionView);
    }

    @Override
    @Transactional
    public boolean updateFunction(String instanceName, String functionName, FunctionView updateView, MultipartFile functionFile) {
        FunctionView oldView = getByName(instanceName, functionName);
        if (oldView == null) {
            throw new GeaflowIllegalException("Function name {} not exists", functionName);
        }

        if (functionFile != null) {
            updateView.setJarPackage(updateJarFile(updateView, functionFile));
        }
        return updateById(oldView.getId(), updateView);
    }

    @Transactional
    @Override
    public boolean deleteFunction(String instanceName, String functionName) {
        String instanceId = getInstanceIdByName(instanceName);
        GeaflowFunction function = functionService.getByName(instanceId, functionName);
        if (function == null) {
            return false;
        }

        GeaflowRemoteFile file = function.getJarPackage();
        if (file != null) {
            // do not delete if file is used by others
            try {
                remoteFileManager.deleteFunctionJar(file.getId(), function.getId());

            } catch (Exception e) {
                log.info(" Delete function -> delete file {} failed ", file.getName(), e);
            }
        }
        return super.dropByName(instanceName, functionName);
    }


    private RemoteFileView createRemoteFile(MultipartFile functionFile) {
        if (!StringUtils.endsWith(functionFile.getOriginalFilename(), JAR_FILE_SUFFIX)) {
            throw new GeaflowIllegalException("Invalid jar file");
        }

        String fileName = functionFile.getOriginalFilename();
        if (remoteFileService.existName(fileName)) {
            throw new GeaflowException("FileName {} exists", fileName);
        }

        String path = RemoteFileStorage.getUserFilePath(ContextHolder.get().getUserId(), fileName);

        RemoteFileView remoteFileView = new RemoteFileView();
        remoteFileView.setName(fileName);
        remoteFileView.setPath(path);
        remoteFileManager.create(remoteFileView, functionFile);

        return remoteFileView;
    }

    private RemoteFileView updateJarFile(FunctionView functionView, MultipartFile multipartFile) {
        if (!StringUtils.endsWith(multipartFile.getOriginalFilename(), JAR_FILE_SUFFIX)) {
            throw new GeaflowIllegalException("Invalid jar file");
        }

        RemoteFileView remoteFileView = functionView.getJarPackage();
        if (remoteFileView == null) {
            return createRemoteFile(multipartFile);

        } else {
            String remoteFileId = remoteFileView.getId();
            remoteFileManager.upload(remoteFileId, multipartFile);
            return remoteFileView;
        }
    }
}
