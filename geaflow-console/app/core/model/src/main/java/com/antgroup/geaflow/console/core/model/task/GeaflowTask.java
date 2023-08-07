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

package com.antgroup.geaflow.console.core.model.task;

import com.antgroup.geaflow.console.common.util.Fmt;
import com.antgroup.geaflow.console.common.util.context.GeaflowContext;
import com.antgroup.geaflow.console.common.util.exception.GeaflowIllegalException;
import com.antgroup.geaflow.console.common.util.type.GeaflowTaskStatus;
import com.antgroup.geaflow.console.common.util.type.GeaflowTaskType;
import com.antgroup.geaflow.console.core.model.GeaflowId;
import com.antgroup.geaflow.console.core.model.data.GeaflowFunction;
import com.antgroup.geaflow.console.core.model.file.GeaflowRemoteFile;
import com.antgroup.geaflow.console.core.model.job.GeaflowJob;
import com.antgroup.geaflow.console.core.model.plugin.config.GeaflowPluginConfig;
import com.antgroup.geaflow.console.core.model.release.GeaflowRelease;
import com.antgroup.geaflow.console.core.model.task.schedule.GeaflowSchedule;
import com.antgroup.geaflow.console.core.model.version.GeaflowVersion;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.collections.CollectionUtils;

@Getter
@Setter
public class GeaflowTask extends GeaflowId {

    public static final String CODE_TASK_MAIN_CLASS = "com.antgroup.geaflow.dsl.runtime.engine.GeaFlowGqlClient";

    private GeaflowRelease release;

    private GeaflowTaskType type;

    private GeaflowSchedule schedule;

    private GeaflowTaskStatus status;

    private String token;

    private Date startTime;

    private Date endTime;

    private GeaflowTaskHandle handle;

    private String host;

    private GeaflowPluginConfig runtimeMetaPluginConfig;

    private GeaflowPluginConfig haMetaPluginConfig;

    private GeaflowPluginConfig metricPluginConfig;

    private GeaflowPluginConfig dataPluginConfig;

    public static String getTaskFileUrlFormatter(String gatewayUrl, String path) {
        return Fmt.as("{}{}/tasks/%s/files?path={}", gatewayUrl, GeaflowContext.API_PREFIX, path);
    }

    @Override
    public void validate() {
        super.validate();
        Preconditions.checkNotNull(runtimeMetaPluginConfig, "Invalid runtimeMetaPluginConfig");
        Preconditions.checkNotNull(haMetaPluginConfig, "Invalid haMetaPluginConfig");
        Preconditions.checkNotNull(metricPluginConfig, "Invalid metricPluginConfig");
        Preconditions.checkNotNull(dataPluginConfig, "Invalid dataPluginConfig");
        Preconditions.checkNotNull(release, "Invalid release");
    }

    public String getMainClass() {
        GeaflowJob job = release.getJob();
        switch (type) {
            case CODE:
                return CODE_TASK_MAIN_CLASS;
            case API:
                return job.getEntryClass();
            default:
                throw new GeaflowIllegalException("Task type {} not supported", type);
        }
    }

    public List<TaskFile> getVersionFiles(String gatewayUrl) {
        List<TaskFile> files = new ArrayList<>();
        getVersionJars().forEach(jar -> files.add(new TaskFile(jar.getUrl(), jar.getMd5())));
        return rewriteTaskFileUrl(files, gatewayUrl);
    }

    public List<TaskFile> getUserFiles(String gatewayUrl) {
        List<TaskFile> files = new ArrayList<>();
        files.add(new TaskFile(release.getUrl(), release.getMd5()));
        getUserJars().forEach(jar -> files.add(new TaskFile(jar.getUrl(), jar.getMd5())));

        return rewriteTaskFileUrl(files, gatewayUrl);
    }

    public List<GeaflowRemoteFile> getVersionJars() {
        List<GeaflowRemoteFile> jars = new ArrayList<>();

        GeaflowVersion version = release.getVersion();
        GeaflowRemoteFile engineJarPackage = version.getEngineJarPackage();
        GeaflowRemoteFile langJarPackage = version.getLangJarPackage();
        Preconditions.checkNotNull(engineJarPackage, "Invalid engine jar of version %s", version.getName());

        jars.add(engineJarPackage);
        if (langJarPackage != null) {
            jars.add(langJarPackage);
        }

        return jars;
    }

    public List<GeaflowRemoteFile> getUserJars() {
        List<GeaflowRemoteFile> jars = new ArrayList<>();

        GeaflowJob job = release.getJob();
        GeaflowRemoteFile jarPackage = job.getJarPackage();
        List<GeaflowFunction> functions = job.getFunctions();

        if (jarPackage != null) {
            jars.add(jarPackage);
        }

        if (CollectionUtils.isNotEmpty(functions)) {
            functions.forEach(f -> {
                GeaflowRemoteFile functionJarPackage = f.getJarPackage();
                Preconditions.checkNotNull(functionJarPackage, "Invalid jar of function %s", f.getName());
                jars.add(functionJarPackage);
            });
        }

        return jars;
    }

    public String getStartupNotifyUrl(String gatewayUrl) {
        return String.format("%s%s/tasks/%s/startup-notify", gatewayUrl, GeaflowContext.API_PREFIX, id);
    }

    public String getTaskFileUrl(String gatewayUrl, String path) {
        return String.format(getTaskFileUrlFormatter(gatewayUrl, path), id);
    }

    private List<TaskFile> rewriteTaskFileUrl(List<TaskFile> files, String gatewayUrl) {
        files.forEach(f -> {
            if (f.getUrl().startsWith(gatewayUrl)) {
                f.setUrl(String.format(f.getUrl(), id));
            }
        });
        return files;
    }
}
