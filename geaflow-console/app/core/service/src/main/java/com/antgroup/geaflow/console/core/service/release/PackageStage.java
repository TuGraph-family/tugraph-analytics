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

package com.antgroup.geaflow.console.core.service.release;

import com.alibaba.fastjson.JSON;
import com.antgroup.geaflow.console.common.util.Md5Util;
import com.antgroup.geaflow.console.common.util.ZipUtil;
import com.antgroup.geaflow.console.common.util.ZipUtil.GeaflowZipEntry;
import com.antgroup.geaflow.console.common.util.exception.GeaflowException;
import com.antgroup.geaflow.console.core.model.release.GeaflowRelease;
import com.antgroup.geaflow.console.core.model.release.JobPlanBuilder;
import com.antgroup.geaflow.console.core.model.release.ReleaseUpdate;
import com.antgroup.geaflow.console.core.service.file.RemoteFileStorage;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class PackageStage extends GeaflowBuildStage {

    private static final String GQL_FILE_NAME = "user.gql";
    private static final String CONF_FILE_NAME = "user.conf";

    @Autowired
    private RemoteFileStorage remoteFileStorage;

    @Override
    public void init(GeaflowRelease release) {
        try {
            List<GeaflowZipEntry> zipEntries;
            if (release.getJob().isApiJob()) {
                GeaflowZipEntry entry = new GeaflowZipEntry("empty.txt", "");
                zipEntries = Collections.singletonList(entry);

            } else {
                String code = release.getJob().getUserCode().getText();
                Map<String, Integer> parallelismMap = JobPlanBuilder.getParallelismMap(release.getJobPlan());
                GeaflowZipEntry codeEntry = new GeaflowZipEntry(GQL_FILE_NAME, code);
                GeaflowZipEntry confEntry = new GeaflowZipEntry(CONF_FILE_NAME, JSON.toJSONString(parallelismMap));
                zipEntries = Arrays.asList(codeEntry, confEntry);
            }
            // url and md5
            String path = RemoteFileStorage.getPackageFilePath(release.getJob().getId(), release.getReleaseVersion());
            release.setUrl(remoteFileStorage.upload(path, ZipUtil.buildZipInputStream(zipEntries)));
            release.setMd5(Md5Util.encodeFile(ZipUtil.buildZipInputStream(zipEntries)));
        } catch (IOException e) {
            throw new GeaflowException("Package job {} fail ", release.getJob().getName(), e);
        }
    }

    @Override
    public boolean update(GeaflowRelease release, ReleaseUpdate update) {
        return false;
    }
}
