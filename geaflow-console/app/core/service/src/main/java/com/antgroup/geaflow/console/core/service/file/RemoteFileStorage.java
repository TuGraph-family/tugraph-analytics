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

import com.antgroup.geaflow.console.common.util.exception.GeaflowIllegalException;
import com.antgroup.geaflow.console.common.util.type.GeaflowPluginCategory;
import com.antgroup.geaflow.console.core.model.plugin.GeaflowPlugin;
import com.antgroup.geaflow.console.core.model.plugin.config.GeaflowPluginConfig;
import com.antgroup.geaflow.console.core.service.PluginConfigService;
import com.antgroup.geaflow.console.core.service.PluginService;
import com.antgroup.geaflow.console.core.service.config.DeployConfig;
import java.io.InputStream;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class RemoteFileStorage {

    private static final String GEAFLOW_FILE_DIRECTORY = "geaflow/files";

    private static final String USER_FILE_PATH_FORMAT = GEAFLOW_FILE_DIRECTORY + "/users/%s/%s";

    private static final String VERSION_FILE_PATH_FORMAT = GEAFLOW_FILE_DIRECTORY + "/versions/%s/%s";

    private static final String PLUGIN_FILE_PATH_FORMAT = GEAFLOW_FILE_DIRECTORY + "/plugins/%s/%s";

    private static final String GEAFLOW_PACKAGE_PATH_FORMAT = "geaflow/packages/%s/release-%s.zip";

    @Autowired
    private PluginService pluginService;

    @Autowired
    private PluginConfigService pluginConfigService;

    @Autowired
    private DeployConfig deployConfig;

    private volatile RemoteFileClient remoteFileClient;

    public static String getUserFilePath(String userId, String fileName) {
        return String.format(USER_FILE_PATH_FORMAT, userId, fileName);
    }

    public static String getVersionFilePath(String versionName, String fileName) {
        return String.format(VERSION_FILE_PATH_FORMAT, versionName, fileName);
    }

    public static String getPluginFilePath(String pluginName, String fileName) {
        return String.format(PLUGIN_FILE_PATH_FORMAT, pluginName, fileName);
    }

    public static String getPackageFilePath(String jobId, int releaseVersion) {
        return String.format(GEAFLOW_PACKAGE_PATH_FORMAT, jobId, releaseVersion);
    }

    public String upload(String path, InputStream stream) {
        checkRemoteFileClient();
        String url = remoteFileClient.getUrl(path);
        log.info("Start upload file, url={}", url);
        remoteFileClient.upload(path, stream);
        log.info("Upload success, url={}", url);
        return url;
    }

    public InputStream download(String path) {
        checkRemoteFileClient();
        log.info("Start download file, url={}", remoteFileClient.getUrl(path));
        return remoteFileClient.download(path);
    }

    public void delete(String path) {
        checkRemoteFileClient();
        log.info("Start delete file, url={}", remoteFileClient.getUrl(path));
        remoteFileClient.delete(path);
    }

    public String getUrl(String path) {
        checkRemoteFileClient();
        return remoteFileClient.getUrl(path);
    }

    public void reset() {
        if (remoteFileClient != null) {
            synchronized (RemoteFileStorage.class) {
                remoteFileClient = null;
            }
        }
    }

    private void checkRemoteFileClient() {
        if (remoteFileClient != null) {
            return;
        }

        synchronized (RemoteFileStorage.class) {
            if (remoteFileClient == null) {
                GeaflowPluginCategory category = GeaflowPluginCategory.REMOTE_FILE;
                GeaflowPlugin plugin = pluginService.getDefaultSystemPlugin(category);
                GeaflowPluginConfig config = pluginConfigService.getDefaultPluginConfig(category, plugin.getType());

                RemoteFileClient client;
                switch (config.getType()) {
                    case LOCAL:
                        client = new LocalFileClient(deployConfig.getGatewayUrl());
                        break;
                    case OSS:
                        client = new OssFileClient();
                        break;
                    case DFS:
                        client = new DfsFileClient();
                        break;
                    default:
                        throw new GeaflowIllegalException("Remote file client type {} not supported", plugin.getType());
                }

                client.init(plugin, config);
                remoteFileClient = client;

                log.info("Init remote file {} client success", plugin.getType());
            }
        }
    }
}
