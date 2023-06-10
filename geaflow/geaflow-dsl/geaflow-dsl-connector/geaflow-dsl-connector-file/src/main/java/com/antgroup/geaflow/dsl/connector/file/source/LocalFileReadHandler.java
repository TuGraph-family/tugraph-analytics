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

import static com.antgroup.geaflow.dsl.connector.file.FileConstants.PREFIX_LOCAL_FILE;

import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.dsl.common.exception.GeaFlowDSLException;
import com.antgroup.geaflow.dsl.connector.api.Partition;
import com.antgroup.geaflow.dsl.connector.file.source.FileTableSource.FileOffset;
import com.antgroup.geaflow.dsl.connector.file.source.FileTableSource.FileSplit;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LocalFileReadHandler extends AbstractFileReadHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(LocalFileReadHandler.class);

    private String path;

    private Map<FileSplit, BufferedReader> readers;

    @Override
    public void init(Configuration conf, String path) {
        if (path.startsWith(PREFIX_LOCAL_FILE)) {
            this.path = path.substring(PREFIX_LOCAL_FILE.length());
        } else {
            throw new GeaFlowDSLException("Local file path should start with: " + PREFIX_LOCAL_FILE);
        }
        this.readers = new HashMap<>();
    }

    @Override
    public List<Partition> listPartitions() {
        File file = new File(path);
        if (file.isDirectory()) {
            Collection<File> files = FileUtils.listFiles(file, null, true);
            List<Partition> partitions = new ArrayList<>();
            for (File f : files) {
                if (f.isHidden()) {
                    continue;
                }
                String relativePath = f.getAbsolutePath().substring(path.length());
                FileSplit partition = new FileSplit(path, relativePath);
                partitions.add(partition);
            }
            return partitions;
        } else {
            String relativePath = file.getAbsolutePath().substring(path.length());
            FileSplit partition = new FileSplit(path, relativePath);
            return Collections.singletonList(partition);
        }
    }

    @Override
    protected BufferedReader getPartitionReader(FileSplit split, FileOffset offset) {
        try {
            if (!readers.containsKey(split)) {
                FileInputStream inputStream = new FileInputStream(split.getPath());
                BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
                reader.skip(offset.getOffset());
                readers.put(split, reader);
            }
            return readers.get(split);
        } catch (IOException e) {
            throw new GeaFlowDSLException(e);
        }
    }


    @Override
    public void close() {
        for (BufferedReader reader : readers.values()) {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e) {
                    LOGGER.warn("Error in close reader", e);
                }
            }
        }
    }
}
