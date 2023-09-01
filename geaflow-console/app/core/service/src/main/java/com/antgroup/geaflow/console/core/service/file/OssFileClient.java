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

package com.antgroup.geaflow.console.core.service.file;

import com.aliyun.oss.OSSClient;
import com.aliyun.oss.OSSException;
import com.aliyun.oss.common.auth.DefaultCredentialProvider;
import com.aliyun.oss.model.OSSObject;
import com.antgroup.geaflow.console.common.util.NetworkUtil;
import com.antgroup.geaflow.console.common.util.exception.GeaflowException;
import com.antgroup.geaflow.console.core.model.plugin.GeaflowPlugin;
import com.antgroup.geaflow.console.core.model.plugin.config.GeaflowPluginConfig;
import com.antgroup.geaflow.console.core.model.plugin.config.OssPluginConfigClass;
import java.io.InputStream;
import org.apache.commons.lang3.StringUtils;

public class OssFileClient implements RemoteFileClient {

    private OssPluginConfigClass ossConfig;

    private OSSClient ossClient;

    @Override
    public void init(GeaflowPlugin plugin, GeaflowPluginConfig config) {
        this.ossConfig = config.getConfig().parse(OssPluginConfigClass.class);
        this.ossClient = new OSSClient(ossConfig.getEndpoint(),
            new DefaultCredentialProvider(ossConfig.getAccessId(), ossConfig.getSecretKey()), null);
    }

    @Override
    public void upload(String path, InputStream inputStream) {
        this.ossClient.putObject(this.ossConfig.getBucket(), getFullPath(path), inputStream);
    }

    @Override
    public InputStream download(String path) throws OSSException {
        OSSObject ossObject = this.ossClient.getObject(this.ossConfig.getBucket(), getFullPath(path));
        return ossObject.getObjectContent();
    }

    @Override
    public void delete(String path) {
        this.ossClient.deleteObject(this.ossConfig.getBucket(), getFullPath(path));
    }

    @Override
    public String getUrl(String path) {
        return String.format("http://%s.%s/%s", ossConfig.getBucket(), NetworkUtil.getHost(ossConfig.getEndpoint()),
            getFullPath(path));
    }

    public String getFullPath(String path) {
        String root = ossConfig.getRoot();
        if (!StringUtils.startsWith(root, "/")) {
            throw new GeaflowException("Invalid root config, should start with /");
        }

        root = StringUtils.removeStart(root, "/");
        root = StringUtils.removeEnd(root, "/");

        return root.isEmpty() ? path : String.format("%s/%s", root, path);
    }

}
