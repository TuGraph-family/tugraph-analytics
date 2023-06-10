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

package com.antgroup.geaflow.dsl.connector.file.sink;

import static com.antgroup.geaflow.dsl.connector.file.FileConstants.PREFIX_LOCAL_FILE;

import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.utils.FileUtil;
import com.antgroup.geaflow.dsl.common.exception.GeaFlowDSLException;
import com.antgroup.geaflow.dsl.common.types.StructType;
import com.antgroup.geaflow.dsl.connector.file.FileConnectorUtil;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LocalFileWriteHandler implements FileWriteHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(LocalFileWriteHandler.class);

    private BufferedWriter writer;

    private final String baseDir;

    public LocalFileWriteHandler(String baseDir) {
        if (baseDir.startsWith(PREFIX_LOCAL_FILE)) {
            this.baseDir = baseDir.substring(PREFIX_LOCAL_FILE.length());
        } else {
            this.baseDir = baseDir;
        }
    }

    @Override
    public void init(Configuration conf, StructType schema, int taskIndex) {
        File dir = new File(baseDir);
        String filePath = FileUtil.concatPath(baseDir, FileConnectorUtil.getPartitionFileName(taskIndex));
        try {
            if (!dir.exists()) {
                dir.mkdirs();
            }
            File file = new File(filePath);
            if (file.exists()) {
                String newPath = filePath + "_" + System.currentTimeMillis();
                file = new File(newPath);
                LOGGER.info("path {} exists, create new file path {}", filePath, newPath);
            }
            file.createNewFile();
            this.writer = new BufferedWriter(new FileWriter(file));
            LOGGER.info("succeed to create file: {}", filePath);
        } catch (IOException e) {
            throw new GeaFlowDSLException("Error in create file: " + filePath, e);
        }
    }

    @Override
    public void write(String text) throws IOException {
        writer.write(text);
    }

    @Override
    public void flush() throws IOException {
        writer.flush();
    }

    @Override
    public void close() throws IOException {
        if (writer != null) {
            writer.close();
        }
    }
}
