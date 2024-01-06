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

package com.antgroup.geaflow.dsl.connector.file.source;

import static com.antgroup.geaflow.dsl.connector.file.FileConstants.PREFIX_JAVA_RESOURCE;

import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.dsl.common.exception.GeaFlowDSLException;
import com.antgroup.geaflow.dsl.common.types.TableSchema;
import com.antgroup.geaflow.dsl.connector.api.Partition;
import com.antgroup.geaflow.dsl.connector.file.source.FileTableSource.FileSplit;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Collections;
import java.util.List;

public class JarFileReadHandler extends AbstractFileReadHandler {

    private String path;

    @Override
    public void init(Configuration tableConf, TableSchema tableSchema, String path) throws IOException {
        super.init(tableConf, tableSchema, path);
        this.path = path.substring(PREFIX_JAVA_RESOURCE.length());
    }

    @Override
    public List<Partition> listPartitions() {
        int index = path.lastIndexOf('/');
        String baseDir = path.substring(0, index);
        String fileName = path.substring(index + 1);
        return Collections.singletonList(new ResourceFileSplit(baseDir, fileName));
    }

    public static class ResourceFileSplit extends FileSplit {

        public ResourceFileSplit(String baseDir, String relativePath) {
            super(baseDir, relativePath);
        }

        @Override
        public InputStream openStream(Configuration conf) throws IOException {
            URL url = getClass().getResource(getPath());
            if (url == null) {
                throw new GeaFlowDSLException("Resource: {} not found", getPath());
            }
            return url.openStream();
        }
    }
}
