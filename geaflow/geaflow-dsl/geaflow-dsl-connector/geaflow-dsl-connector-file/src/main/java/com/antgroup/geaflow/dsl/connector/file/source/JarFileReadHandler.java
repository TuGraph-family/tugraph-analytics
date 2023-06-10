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
import com.antgroup.geaflow.dsl.connector.api.Partition;
import com.antgroup.geaflow.dsl.connector.file.source.FileTableSource.FileOffset;
import com.antgroup.geaflow.dsl.connector.file.source.FileTableSource.FileSplit;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.Collections;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JarFileReadHandler extends AbstractFileReadHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(JarFileReadHandler.class);

    private String path;

    private BufferedReader reader;

    @Override
    public void init(Configuration conf, String path) {
        this.path = path.substring(PREFIX_JAVA_RESOURCE.length());
    }

    @Override
    public List<Partition> listPartitions() {
        int index = path.lastIndexOf('/');
        String baseDir = path.substring(0, index);
        String fileName = path.substring(index + 1);
        return Collections.singletonList(new FileSplit(baseDir, fileName));
    }

    @Override
    public void close() {
        if (reader != null) {
            try {
                reader.close();
            } catch (IOException e) {
                LOGGER.warn("Error in close reader", e);
            }
        }
    }

    @Override
    protected BufferedReader getPartitionReader(FileSplit split, FileOffset offset) {
        try {
            if (reader == null) {
                URL url = getClass().getResource(path);
                assert url != null;
                reader = new BufferedReader(new InputStreamReader(url.openStream()));
                reader.skip(offset.getOffset());
            }
            return reader;
        } catch (IOException e) {
            throw new GeaFlowDSLException(e);
        }
    }
}
