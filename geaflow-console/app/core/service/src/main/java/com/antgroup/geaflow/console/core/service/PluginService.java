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

import com.antgroup.geaflow.console.common.dal.dao.NameDao;
import com.antgroup.geaflow.console.common.dal.dao.PluginDao;
import com.antgroup.geaflow.console.common.dal.entity.PluginEntity;
import com.antgroup.geaflow.console.common.dal.model.PluginSearch;
import com.antgroup.geaflow.console.common.service.integration.engine.GeaflowCompiler;
import com.antgroup.geaflow.console.common.util.exception.GeaflowCompileException;
import com.antgroup.geaflow.console.common.util.exception.GeaflowException;
import com.antgroup.geaflow.console.common.util.type.GeaflowPluginCategory;
import com.antgroup.geaflow.console.core.model.file.GeaflowRemoteFile;
import com.antgroup.geaflow.console.core.model.plugin.GeaflowPlugin;
import com.antgroup.geaflow.console.core.model.version.GeaflowVersion;
import com.antgroup.geaflow.console.core.service.converter.NameConverter;
import com.antgroup.geaflow.console.core.service.converter.PluginConverter;
import com.antgroup.geaflow.console.core.service.file.FileRefService;
import com.antgroup.geaflow.console.core.service.version.CompileClassLoader;
import com.antgroup.geaflow.console.core.service.version.FunctionClassLoader;
import com.antgroup.geaflow.console.core.service.version.VersionClassLoader;
import com.antgroup.geaflow.console.core.service.version.VersionFactory;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.io.FileUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

@Service
public class PluginService extends NameService<GeaflowPlugin, PluginEntity, PluginSearch> implements FileRefService {


    public static final String PLUGIN_DEFAULT_INSTANCE_ID = "0";

    @Autowired
    private PluginDao pluginDao;

    @Autowired
    private PluginConverter pluginConverter;

    @Autowired
    private RemoteFileService remoteFileService;

    @Autowired
    private VersionFactory versionFactory;


    @Override
    protected NameDao<PluginEntity, PluginSearch> getDao() {
        return pluginDao;
    }

    @Override
    protected NameConverter<GeaflowPlugin, PluginEntity> getConverter() {
        return pluginConverter;
    }

    @Override
    protected List<GeaflowPlugin> parse(List<PluginEntity> pluginEntities) {
        return pluginEntities.stream().map(e -> {
            GeaflowRemoteFile jarPackage = remoteFileService.get(e.getJarPackageId());
            return pluginConverter.convert(e, jarPackage);
        }).collect(Collectors.toList());
    }

    public List<GeaflowPlugin> getPlugins(GeaflowPluginCategory category) {
        List<PluginEntity> plugins = pluginDao.getPlugins(category);
        return parse(plugins);
    }

    public GeaflowPlugin getPlugin(String type, GeaflowPluginCategory category) {
        PluginEntity plugin = pluginDao.getPlugin(type, category);
        return plugin == null ? null : parse(plugin);
    }

    public GeaflowPlugin getDefaultPlugin(GeaflowPluginCategory category) {
        List<GeaflowPlugin> plugins = getPlugins(category);
        if (plugins.size() != 1) {
            throw new GeaflowException("At least one plugin for {} plugin needed", category);
        }

        return plugins.get(0);
    }

    public List<GeaflowPlugin> getSystemPlugins(GeaflowPluginCategory category) {
        List<PluginEntity> plugins = pluginDao.getSystemPlugins(category);
        return parse(plugins);
    }

    public GeaflowPlugin getDefaultSystemPlugin(GeaflowPluginCategory category) {
        List<GeaflowPlugin> plugins = getSystemPlugins(category);
        if (plugins.size() != 1) {
            throw new GeaflowException("At least one system plugin for {} plugin needed", category);
        }

        return plugins.get(0);
    }

    public void validateGetIds(List<String> ids) {
        for (String id : ids) {
            if (!pluginDao.validateGetId(id)) {
                throw new GeaflowException("Invalidate id {} (Not system session or current user is not the creator)", id);
            }
        }
    }

    public void validateUpdateIds(List<String> ids) {
        for (String id : ids) {
            if (!pluginDao.validateUpdateId(id)) {
                throw new GeaflowException("Invalidate id {} (Not system session or current user is not the creator)", id);
            }
        }
    }


    public boolean pluginTypeInEngine(String pluginType, GeaflowVersion version) {
        VersionClassLoader classLoader = versionFactory.getClassLoader(version);
        return checkPluginType(classLoader, pluginType);
    }

    @Override
    public long getFileRefCount(String jarId, String pluginId) {
        return pluginDao.getFileRefCount(jarId, pluginId);
    }

    public void checkJar(String type, MultipartFile jarPackage, GeaflowVersion version) {
        FunctionClassLoader functionClassLoader = null;
        File file = null;
        try {
            String tmpPath = "/tmp/geaflow/tmpFile/" + jarPackage.getOriginalFilename();
            file = new File(tmpPath);
            FileUtils.copyInputStreamToFile(jarPackage.getInputStream(), file);
            URL url = file.toURI().toURL();
            VersionClassLoader versionClassLoader = versionFactory.getClassLoader(version);
            functionClassLoader = new FunctionClassLoader(versionClassLoader, new URL[]{url});
            if (!checkPluginType(functionClassLoader, type)) {
                throw new GeaflowException("Plugin type {} is not in the jar", type);
            }

        } catch (IOException e) {
            throw new RuntimeException(e);

        } finally {
            if (functionClassLoader != null) {
                functionClassLoader.closeClassLoader();
            }

            if (file != null) {
                file.delete();
            }
        }
    }

    public void checkJar(String type, String jarId, GeaflowVersion version) {
        FunctionClassLoader functionClassLoader = null;
        try {
            GeaflowRemoteFile remoteFile = remoteFileService.get(jarId);
            VersionClassLoader versionClassLoader = versionFactory.getClassLoader(version);
            functionClassLoader = new FunctionClassLoader(versionClassLoader, Arrays.asList(remoteFile));
            if (!checkPluginType(functionClassLoader, type)) {
                throw new GeaflowException("Plugin type {} is not in the jar", type);
            }
        } finally {
            if (functionClassLoader != null) {
                functionClassLoader.closeClassLoader();
            }
        }
    }

    private boolean checkPluginType(CompileClassLoader classLoader, String type) {
        try {
            GeaflowCompiler compiler = classLoader.newInstance(GeaflowCompiler.class);
            Set<String> enginePlugins = compiler.getEnginePlugins();
            return enginePlugins.contains(type.toUpperCase());
        } catch (Exception e) {
            throw new GeaflowCompileException("Compile job code failed", e);
        }
    }
}
