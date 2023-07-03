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

import com.antgroup.geaflow.console.biz.shared.RemoteFileManager;
import com.antgroup.geaflow.console.biz.shared.convert.NameViewConverter;
import com.antgroup.geaflow.console.biz.shared.convert.RemoteFileViewConverter;
import com.antgroup.geaflow.console.biz.shared.view.RemoteFileView;
import com.antgroup.geaflow.console.common.dal.entity.RemoteFileEntity;
import com.antgroup.geaflow.console.common.dal.model.RemoteFileSearch;
import com.antgroup.geaflow.console.common.util.HTTPUtil;
import com.antgroup.geaflow.console.common.util.ListUtil;
import com.antgroup.geaflow.console.common.util.Md5Util;
import com.antgroup.geaflow.console.common.util.exception.GeaflowException;
import com.antgroup.geaflow.console.common.util.type.GeaflowFileType;
import com.antgroup.geaflow.console.core.model.file.GeaflowRemoteFile;
import com.antgroup.geaflow.console.core.service.FunctionService;
import com.antgroup.geaflow.console.core.service.JobService;
import com.antgroup.geaflow.console.core.service.NameService;
import com.antgroup.geaflow.console.core.service.RemoteFileService;
import com.antgroup.geaflow.console.core.service.VersionService;
import com.antgroup.geaflow.console.core.service.file.RemoteFileStorage;
import com.google.common.base.Preconditions;
import java.io.InputStream;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import javax.servlet.http.HttpServletResponse;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.multipart.MultipartFile;

@Service
public class RemoteFileManagerImpl extends
    NameManagerImpl<GeaflowRemoteFile, RemoteFileView, RemoteFileSearch> implements RemoteFileManager {

    @Autowired
    private RemoteFileViewConverter remoteFileViewConverter;

    @Autowired
    private RemoteFileService remoteFileService;

    @Autowired
    private RemoteFileStorage remoteFileStorage;

    @Autowired
    private FunctionService functionService;

    @Autowired
    private VersionService versionService;

    @Autowired
    private JobService jobService;

    @Override
    protected NameViewConverter<GeaflowRemoteFile, RemoteFileView> getConverter() {
        return remoteFileViewConverter;
    }

    @Override
    protected NameService<GeaflowRemoteFile, RemoteFileEntity, RemoteFileSearch> getService() {
        return remoteFileService;
    }

    @Override
    protected List<GeaflowRemoteFile> parse(List<RemoteFileView> views) {
        return ListUtil.convert(views, v -> remoteFileViewConverter.convert(v));
    }

    @Transactional
    @Override
    public String create(RemoteFileView view, MultipartFile multipartFile) {
        String name = Optional.ofNullable(view.getName()).orElse(multipartFile.getOriginalFilename());
        String path = Preconditions.checkNotNull(view.getPath(), "Invalid path");

        String remoteFileId;
        try {
            view.setName(name);
            view.setType(GeaflowFileType.of(StringUtils.substringAfterLast(multipartFile.getOriginalFilename(), ".")));
            view.setMd5(Md5Util.encodeFile(multipartFile));
            // set url according to the path
            String url = remoteFileStorage.getUrl(view.getPath());
            view.setUrl(url);
            remoteFileId = create(view);
            view.setId(remoteFileId);
        } catch (Exception e) {
            throw new GeaflowException("Create file {} failed", name, e);
        }

        try (InputStream inputStream = multipartFile.getInputStream()) {
            remoteFileStorage.upload(path, inputStream);

        } catch (Exception e) {
            throw new GeaflowException("Upload file {} failed", name, path, e);
        }

        return remoteFileId;
    }

    @Transactional
    @Override
    public boolean upload(String remoteFileId, MultipartFile multipartFile) {
        remoteFileService.validateUpdateIds(Collections.singletonList(remoteFileId));
        GeaflowRemoteFile remoteFile = remoteFileService.get(remoteFileId);
        if (remoteFile == null) {
            return false;
        }

        String name = remoteFile.getName();
        String path = remoteFile.getPath();

        try {
            String md5 = Md5Util.encodeFile(multipartFile);
            if (md5.equals(remoteFile.getMd5())) {
                return false;
            }
            remoteFileService.updateMd5ById(remoteFileId, md5);
            remoteFileService.updateUrlById(remoteFileId, remoteFileStorage.getUrl(path));
        } catch (Exception e) {
            throw new GeaflowException("Update file {} md5 failed", name, e);
        }

        try (InputStream inputStream = multipartFile.getInputStream()) {
            remoteFileStorage.upload(path, inputStream);

        } catch (Exception e) {
            throw new GeaflowException("Upload file {} failed", name, path, e);
        }

        return true;
    }

    @Override
    public boolean download(String remoteFileId, HttpServletResponse response) {
        remoteFileService.validateGetIds(Collections.singletonList(remoteFileId));
        GeaflowRemoteFile remoteFile = remoteFileService.get(remoteFileId);
        if (remoteFile == null) {
            throw new GeaflowException("File not found");
        }

        String path = remoteFile.getPath();
        String name = remoteFile.getName();

        try (InputStream input = remoteFileStorage.download(path)) {
            HTTPUtil.download(response, input, name);

        } catch (Exception e) {
            throw new GeaflowException("Download file {} from {} failed", name, path, e);
        }

        return true;
    }

    @Override
    public boolean delete(String remoteFileId) {
        remoteFileService.validateUpdateIds(Collections.singletonList(remoteFileId));
        // throw exception if file is used by others
        checkFileUsed(remoteFileId);
        return deleteFile(remoteFileId);
    }

    @Override
    public List<RemoteFileView> get(List<String> ids) {
        remoteFileService.validateGetIds(ids);
        return super.get(ids);
    }

    @Override
    public void deleteFunctionJar(String jarId, String functionId) {
        if (jarId == null) {
            return;
        }

        long functionCount = functionService.getFileRefCount(jarId, functionId);
        if (functionCount > 0) {
            return;
        }

        long jobCount = jobService.getFileRefCount(jarId, null);
        if (jobCount > 0) {
            return;
        }

        deleteFile(jarId);
    }

    @Override
    public void deleteJobJar(String jarId, String jobId) {
        if (jarId == null) {
            return;
        }

        long functionCount = functionService.getFileRefCount(jarId, null);
        if (functionCount > 0) {
            return;
        }

        long jobCount = jobService.getFileRefCount(jarId, jobId);
        if (jobCount > 0) {
            return;
        }

        deleteFile(jarId);
    }

    @Override
    public void deleteVersionJar(String jarId) {
        // version jar is only used by a version
        deleteFile(jarId);
    }


    private void checkFileUsed(String remoteFileId) {
        long functionCount = functionService.getFileRefCount(remoteFileId, null);
        if (functionCount > 0) {
            throw new GeaflowException("file is used by functions, count:{}", remoteFileId, functionCount);
        }

        long jobCount = jobService.getFileRefCount(remoteFileId, null);
        if (jobCount > 0) {
            throw new GeaflowException("file is used by jobs, count:{}", remoteFileId, jobCount);
        }

        long versionCount = versionService.getFileRefCount(remoteFileId, null);
        if (versionCount > 0) {
            throw new GeaflowException("file is used by versions, count:{}", remoteFileId, versionCount);
        }
    }

    private boolean deleteFile(String remoteFileId) {
        GeaflowRemoteFile remoteFile = remoteFileService.get(remoteFileId);
        if (remoteFile == null) {
            return false;
        }

        String id = remoteFile.getId();
        String name = remoteFile.getName();
        String path = remoteFile.getPath();

        try {
            remoteFileStorage.delete(path);
            drop(id);

        } catch (Exception e) {
            throw new GeaflowException("Delete file {} failed", name, e);
        }

        return true;
    }

}
