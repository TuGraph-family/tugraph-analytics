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

import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.dsl.common.exception.GeaFlowDSLException;
import com.antgroup.geaflow.dsl.common.types.StructType;
import com.antgroup.geaflow.dsl.connector.file.FileConnectorUtil;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LocalFileWriteHandler implements FileWriteHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(LocalFileWriteHandler.class);

    private final String baseDir;

    private Writer writer;

    public LocalFileWriteHandler(String baseDir) {
        this.baseDir = baseDir;
    }

    @Override
    public void init(Configuration tableConf, StructType schema, int taskIndex) {
        File dirPath = new File(baseDir);
        File filePath = new File(dirPath, FileConnectorUtil.getPartitionFileName(taskIndex));
        try {
            if (!dirPath.exists()) {
                dirPath.mkdirs();
            }
            if (filePath.exists()) {
                String newPath = filePath + "_" + System.currentTimeMillis();
                this.writer = new BufferedWriter(new FileWriter(newPath));
                LOGGER.info("path {} exists, create new file path {}", filePath, newPath);
            } else {
                this.writer = new BufferedWriter(new FileWriter(filePath));
                LOGGER.info("create file path {}", filePath);
            }
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
