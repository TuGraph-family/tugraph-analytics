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

import static com.antgroup.geaflow.console.core.service.PluginService.PLUGIN_DEFAULT_INSTANCE_ID;
import static com.antgroup.geaflow.console.core.service.RemoteFileService.JAR_FILE_SUFFIX;

import com.antgroup.geaflow.console.biz.shared.PluginManager;
import com.antgroup.geaflow.console.biz.shared.RemoteFileManager;
import com.antgroup.geaflow.console.biz.shared.convert.NameViewConverter;
import com.antgroup.geaflow.console.biz.shared.convert.PluginViewConverter;
import com.antgroup.geaflow.console.biz.shared.view.IdView;
import com.antgroup.geaflow.console.biz.shared.view.PluginView;
import com.antgroup.geaflow.console.biz.shared.view.RemoteFileView;
import com.antgroup.geaflow.console.common.dal.model.PluginSearch;
import com.antgroup.geaflow.console.common.util.ListUtil;
import com.antgroup.geaflow.console.common.util.context.ContextHolder;
import com.antgroup.geaflow.console.common.util.exception.GeaflowException;
import com.antgroup.geaflow.console.common.util.exception.GeaflowIllegalException;
import com.antgroup.geaflow.console.common.util.type.GeaflowPluginCategory;
import com.antgroup.geaflow.console.common.util.type.GeaflowResourceType;
import com.antgroup.geaflow.console.core.model.GeaflowName;
import com.antgroup.geaflow.console.core.model.file.GeaflowRemoteFile;
import com.antgroup.geaflow.console.core.model.plugin.GeaflowPlugin;
import com.antgroup.geaflow.console.core.model.plugin.config.GeaflowPluginConfig;
import com.antgroup.geaflow.console.core.model.version.GeaflowVersion;
import com.antgroup.geaflow.console.core.service.JobService;
import com.antgroup.geaflow.console.core.service.NameService;
import com.antgroup.geaflow.console.core.service.PluginConfigService;
import com.antgroup.geaflow.console.core.service.PluginService;
import com.antgroup.geaflow.console.core.service.RemoteFileService;
import com.antgroup.geaflow.console.core.service.VersionService;
import com.antgroup.geaflow.console.core.service.file.RemoteFileStorage;
import com.google.common.base.Preconditions;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.multipart.MultipartFile;

@Service
@Slf4j
public class PluginManagerImpl extends NameManagerImpl<GeaflowPlugin, PluginView, PluginSearch> implements
    PluginManager {

    @Autowired
    private PluginService pluginService;

    @Autowired
    private PluginViewConverter pluginViewConverter;

    @Autowired
    private RemoteFileManager remoteFileManager;

    @Autowired
    private RemoteFileService remoteFileService;

    @Autowired
    private VersionService versionService;

    @Autowired
    private JobService jobService;

    @Autowired
    private PluginConfigService pluginConfigService;

    @Override
    protected NameViewConverter<GeaflowPlugin, PluginView> getConverter() {
        return pluginViewConverter;
    }

    @Override
    protected List<GeaflowPlugin> parse(List<PluginView> views) {
        return views.stream().map(e -> {
            GeaflowRemoteFile jarPackage = remoteFileService.get(
                Optional.ofNullable(e.getJarPackage()).map(IdView::getId).orElse(null));
            return pluginViewConverter.convert(e, jarPackage);
        }).collect(Collectors.toList());
    }

    @Transactional
    @Override
    public String createPlugin(PluginView pluginView, MultipartFile jarPackage, String jarId) {
        String pluginName = pluginView.getName();
        if (StringUtils.isBlank(pluginName)) {
            throw new GeaflowIllegalException("Invalid plugin name");
        }

        if (pluginService.existName(pluginName)) {
            throw new GeaflowIllegalException("Plugin name {} exists", pluginName);
        }

        String type = pluginView.getType();
        GeaflowPluginCategory category = pluginView.getCategory();
        Preconditions.checkNotNull(type, "Invalid plugin name type");
        Preconditions.checkNotNull(category, "Invalid plugin name category");
        GeaflowVersion defaultVersion = versionService.getDefaultVersion();
        if (category == GeaflowPluginCategory.TABLE) {
            if (jarPackage == null && jarId == null) {
                throw new GeaflowIllegalException("Need upload or bind a jar");
            }
            if (pluginService.pluginTypeInEngine(type, defaultVersion)) {
                throw new GeaflowIllegalException("Plugin type {} of category {} exists in engine", type, category);
            }
        }

        GeaflowPlugin plugin = pluginService.getPlugin(type, category);
        if (plugin != null) {
            throw new GeaflowIllegalException("Plugin type {} of category {} exists", type, category);
        }

        if (jarId == null) {
            if (jarPackage != null) {
                pluginService.checkJar(type, jarPackage, defaultVersion);
                RemoteFileView remoteFile = createRemoteFile(pluginName, jarPackage);
                pluginView.setJarPackage(remoteFile);
            }
        } else {
            pluginService.checkJar(type, jarId, defaultVersion);
            RemoteFileView remoteFileView = new RemoteFileView();
            remoteFileView.setId(jarId);
            pluginView.setJarPackage(remoteFileView);
        }

        return super.create(pluginView);
    }

    @Transactional
    @Override
    public boolean updatePlugin(String pluginId, PluginView updateView, MultipartFile jarPackage) {
        pluginService.validateUpdateIds(Collections.singletonList(pluginId));
        PluginView view = get(pluginId);
        if (view == null) {
            throw new GeaflowIllegalException("plugin id {} not exists", pluginId);
        }

        if (jarPackage != null) {
            RemoteFileView remoteFileView = updateJarPackage(view, jarPackage);
            updateView.setJarPackage(remoteFileView);
        }

        return updateById(view.getId(), updateView);
    }

    private RemoteFileView updateJarPackage(PluginView versionView, MultipartFile multipartFile) {
        if (!StringUtils.endsWith(multipartFile.getOriginalFilename(), JAR_FILE_SUFFIX)) {
            throw new GeaflowIllegalException("Invalid jar file");
        }

        RemoteFileView jarPackage = versionView.getJarPackage();
        if (jarPackage == null) {
            return createRemoteFile(versionView.getName(), multipartFile);

        } else {
            String remoteFileId = jarPackage.getId();
            remoteFileManager.upload(remoteFileId, multipartFile);
            return null;
        }
    }


    private RemoteFileView createRemoteFile(String pluginName, MultipartFile multipartFile) {
        if (!StringUtils.endsWith(multipartFile.getOriginalFilename(), JAR_FILE_SUFFIX)) {
            throw new GeaflowIllegalException("Invalid jar file");
        }

        String fileName = multipartFile.getOriginalFilename();
        boolean systemSession = ContextHolder.get().isSystemSession();
        String userId = ContextHolder.get().getUserId();
        String path = systemSession ? RemoteFileStorage.getPluginFilePath(pluginName, fileName)
                                    : RemoteFileStorage.getUserFilePath(userId, fileName);

        RemoteFileView remoteFileView = new RemoteFileView();
        remoteFileView.setName(fileName);
        remoteFileView.setPath(path);
        remoteFileManager.create(remoteFileView, multipartFile);

        return remoteFileView;
    }

    @Override
    protected NameService<GeaflowPlugin, ?, PluginSearch> getService() {
        return pluginService;
    }

    @Override
    public List<PluginView> get(List<String> ids) {
        pluginService.validateGetIds(ids);
        return super.get(ids);
    }

    @Override
    public boolean drop(List<String> ids) {
        pluginService.validateUpdateIds(ids);

        for (String id : ids) {
            GeaflowPlugin geaflowPlugin = pluginService.get(id);
            // check plugin is used by jobs or tables
            checkPluginUsed(geaflowPlugin);

            GeaflowRemoteFile file = geaflowPlugin.getJarPackage();
            if (file != null) {
                try {
                    remoteFileManager.deleteRefJar(file.getId(), geaflowPlugin.getId(), GeaflowResourceType.PLUGIN);

                } catch (Exception e) {
                    log.info(" Delete plugin file {} failed ", file.getName(), e);
                }
            }
        }

        return super.drop(ids);
    }

    private void checkPluginUsed(GeaflowPlugin geaflowPlugin) {
        List<String> jobIds = jobService.getJobByResources(geaflowPlugin.getName(), PLUGIN_DEFAULT_INSTANCE_ID,
            GeaflowResourceType.PLUGIN);
        if (CollectionUtils.isNotEmpty(jobIds)) {
            List<String> jobNames = ListUtil.convert(jobIds, e -> jobService.getNameById(e));
            throw new GeaflowException("Plugin {} is used by job: {}", geaflowPlugin.getName(), String.join(",", jobNames));
        }

        List<GeaflowPluginConfig> pluginConfigs = pluginConfigService.getPluginConfigs(null, geaflowPlugin.getType());
        if (CollectionUtils.isNotEmpty(pluginConfigs)) {
            List<String> configNames = ListUtil.convert(pluginConfigs, GeaflowName::getName);
            throw new GeaflowException("Plugin {} is used by config: {}", geaflowPlugin.getName(), String.join(",", configNames));
        }
    }

}
