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

import com.antgroup.geaflow.console.common.util.FileUtil;
import com.antgroup.geaflow.console.common.util.exception.GeaflowException;
import com.antgroup.geaflow.console.core.model.plugin.GeaflowPlugin;
import com.antgroup.geaflow.console.core.model.plugin.config.GeaflowPluginConfig;
import com.antgroup.geaflow.console.core.model.plugin.config.LocalPluginConfigClass;
import com.antgroup.geaflow.console.core.model.task.GeaflowTask;
import java.io.InputStream;
import org.apache.commons.lang3.StringUtils;

public class LocalFileClient implements RemoteFileClient {

    private final String gatewayUrl;

    private LocalPluginConfigClass localConfig;

    public LocalFileClient(String gatewayUrl) {
        this.gatewayUrl = gatewayUrl;
    }

    @Override
    public void init(GeaflowPlugin plugin, GeaflowPluginConfig config) {
        this.localConfig = config.getConfig().parse(LocalPluginConfigClass.class);
        FileUtil.mkdir(localConfig.getRoot());
    }

    @Override
    public void upload(String path, InputStream inputStream) {
        FileUtil.writeFile(getFullPath(path), inputStream);
    }

    @Override
    public InputStream download(String path) {
        return FileUtil.readFileStream(getFullPath(path));
    }

    @Override
    public void delete(String path) {
        FileUtil.delete(getFullPath(path));
    }

    @Override
    public String getUrl(String path) {
        return GeaflowTask.getTaskFileUrlFormatter(gatewayUrl, getFullPath(path));
    }

    public String getFullPath(String path) {
        String root = localConfig.getRoot();
        if (!StringUtils.startsWith(root, "/")) {
            throw new GeaflowException("Invalid root config, should start with /");
        }
        root = StringUtils.removeEnd(root, "/");

        return String.format("%s/%s", root, path);
    }

}
