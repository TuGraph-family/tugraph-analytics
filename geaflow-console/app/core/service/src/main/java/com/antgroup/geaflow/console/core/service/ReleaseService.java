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

import com.antgroup.geaflow.console.common.dal.dao.IdDao;
import com.antgroup.geaflow.console.common.dal.dao.ReleaseDao;
import com.antgroup.geaflow.console.common.dal.entity.ReleaseEntity;
import com.antgroup.geaflow.console.common.dal.model.ReleaseSearch;
import com.antgroup.geaflow.console.common.service.integration.engine.CompileContext;
import com.antgroup.geaflow.console.common.service.integration.engine.CompileResult;
import com.antgroup.geaflow.console.common.service.integration.engine.GeaflowCompiler;
import com.antgroup.geaflow.console.common.util.context.ContextHolder;
import com.antgroup.geaflow.console.common.util.exception.GeaflowCompileException;
import com.antgroup.geaflow.console.core.model.cluster.GeaflowCluster;
import com.antgroup.geaflow.console.core.model.data.GeaflowInstance;
import com.antgroup.geaflow.console.core.model.job.GeaflowJob;
import com.antgroup.geaflow.console.core.model.release.GeaflowRelease;
import com.antgroup.geaflow.console.core.model.version.GeaflowVersion;
import com.antgroup.geaflow.console.core.service.converter.IdConverter;
import com.antgroup.geaflow.console.core.service.converter.ReleaseConverter;
import com.antgroup.geaflow.console.core.service.file.RemoteFileStorage;
import com.antgroup.geaflow.console.core.service.version.VersionClassLoader;
import com.antgroup.geaflow.console.core.service.version.VersionFactory;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.collections.MapUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class ReleaseService extends IdService<GeaflowRelease, ReleaseEntity, ReleaseSearch> {

    private static final String GEAFLOW_DSL_CATALOG_TOKEN_KEY = "geaflow.dsl.catalog.token.key";

    private static final String GEAFLOW_DSL_CATALOG_INSTANCE_NAME = "geaflow.dsl.catalog.instance.name";

    @Autowired
    private ReleaseDao releaseDao;

    @Autowired
    private JobService jobService;

    @Autowired
    private ReleaseConverter releaseConverter;

    @Autowired
    private VersionService versionService;

    @Autowired
    private InstanceService instanceService;

    @Autowired
    private ClusterService clusterService;

    @Autowired
    private VersionFactory versionFactory;

    @Autowired
    private RemoteFileStorage remoteFileStorage;

    protected IdDao<ReleaseEntity, ReleaseSearch> getDao() {
        return releaseDao;
    }

    @Override
    protected IdConverter<GeaflowRelease, ReleaseEntity> getConverter() {
        return releaseConverter;
    }

    @Override
    protected List<GeaflowRelease> parse(List<ReleaseEntity> releaseEntities) {
        return releaseEntities.stream().map(e -> {
            GeaflowJob job = jobService.get(e.getJobId());

            GeaflowVersion version = versionService.get(e.getVersionId());

            GeaflowCluster cluster = clusterService.get(e.getClusterId());

            return releaseConverter.convert(e, job, version, cluster);
        }).collect(Collectors.toList());
    }

    public CompileResult compile(GeaflowJob job, GeaflowVersion version, Map<String, Integer> parallelisms) {
        VersionClassLoader classLoader = versionFactory.getClassLoader(version);
        CompileContext context = classLoader.newInstance(CompileContext.class);
        setContextToken(context, job.getInstanceId());
        if (MapUtils.isNotEmpty(parallelisms)) {
            context.setParallelisms(parallelisms);
        }
        GeaflowCompiler compiler = classLoader.newInstance(GeaflowCompiler.class);

        try {
            return compiler.compile(job.generateCode().getText(), context);
        } catch (Exception e) {
            throw new GeaflowCompileException("Compile job code failed", e);
        }
    }

    private void setContextToken(CompileContext context, String instanceId) {
        GeaflowInstance instance = instanceService.get(instanceId);
        Map<String, String> config = new HashMap<>();
        config.put(GEAFLOW_DSL_CATALOG_INSTANCE_NAME, instance.getName());
        config.put(GEAFLOW_DSL_CATALOG_TOKEN_KEY, ContextHolder.get().getSessionToken());
        context.setConfig(config);
    }

    public void dropByJobIds(List<String> ids) {
        List<ReleaseEntity> releases = releaseDao.getByJobIds(ids);
        for (ReleaseEntity release : releases) {
            for (int i = 1; i <= release.getVersion(); i++) {
                String path = RemoteFileStorage.getPackageFilePath(release.getJobId(), i);
                remoteFileStorage.delete(path);
            }
        }
        releaseDao.dropByJobIds(ids);
    }
}

