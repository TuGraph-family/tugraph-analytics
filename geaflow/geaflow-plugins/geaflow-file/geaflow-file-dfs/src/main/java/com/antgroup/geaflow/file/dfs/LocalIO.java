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

package com.antgroup.geaflow.file.dfs;

import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys;
import com.antgroup.geaflow.common.exception.GeaflowRuntimeException;
import com.antgroup.geaflow.file.FileConfigKeys;
import com.antgroup.geaflow.file.PersistentType;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LocalIO extends DfsIO {

    private static final Logger LOGGER = LoggerFactory.getLogger(LocalIO.class);
    private static final String LOCAL = "file:///";

    @Override
    public void init(Configuration userConfig) {
        String root;
        if (userConfig.contains(FileConfigKeys.ROOT)) {
            root = userConfig.getString(FileConfigKeys.ROOT);
        } else {
            root = userConfig.getString(ExecutionConfigKeys.JOB_WORK_PATH)
                + userConfig.getString(FileConfigKeys.ROOT);
        }

        LOGGER.info("use local chk path {}", root);
        try {
            FileUtils.forceMkdir(new File(root));
        } catch (IOException e) {
            throw new GeaflowRuntimeException("mkdir fail " + root, e);
        }

        org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
        conf.set(LOCAL_FILE_IMPL, LocalFileSystem.class.getCanonicalName());
        try {
            this.fileSystem = FileSystem.newInstance(new URI(LOCAL), conf);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public PersistentType getPersistentType() {
        return PersistentType.LOCAL;
    }
}
