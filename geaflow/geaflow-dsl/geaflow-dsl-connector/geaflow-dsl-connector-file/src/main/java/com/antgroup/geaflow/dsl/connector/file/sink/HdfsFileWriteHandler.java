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

import static com.antgroup.geaflow.dsl.connector.file.FileConstants.PREFIX_HDFS;

import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.utils.FileUtil;
import com.antgroup.geaflow.dsl.common.exception.GeaFlowDSLException;
import com.antgroup.geaflow.dsl.common.types.StructType;
import com.antgroup.geaflow.dsl.connector.file.FileConnectorUtil;
import java.io.IOException;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HdfsFileWriteHandler implements FileWriteHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(HdfsFileWriteHandler.class);

    protected Configuration conf;
    protected StructType schema;
    protected int taskIndex;
    protected final String baseDir;

    protected transient FileSystem fileSystem;
    protected transient FSDataOutputStream writer;

    public HdfsFileWriteHandler(String baseDir) {
        if (baseDir.startsWith(PREFIX_HDFS)) {
            this.baseDir = baseDir.substring(PREFIX_HDFS.length());
        } else {
            this.baseDir = baseDir;
        }
    }

    @Override
    public void init(Configuration conf, StructType schema, int taskIndex) {
        this.conf = conf;
        this.schema = schema;
        this.taskIndex = taskIndex;
        this.fileSystem = FileConnectorUtil.getHdfsFileSystem(conf);

        open();
    }

    protected void open() {
        String filePath = FileUtil.concatPath(baseDir, FileConnectorUtil.getPartitionFileName(taskIndex));
        try {
            if (!fileSystem.exists(new Path(baseDir))) {
                fileSystem.mkdirs(new Path(baseDir));
                LOGGER.info("mkdirs {}", baseDir);
            }
            if (fileSystem.exists(new Path(filePath))) {
                String newPath = filePath + "_" + System.currentTimeMillis();
                this.writer = fileSystem.create(new Path(newPath));
                LOGGER.info("path {} exists, create new file path {}", filePath, newPath);
            } else {
                this.writer = fileSystem.create(new Path(filePath));
                LOGGER.info("create file path {}", filePath);
            }
        } catch (IOException e) {
            throw new GeaFlowDSLException("Error in create file: " + filePath, e);
        }
    }

    @Override
    public void write(String text) throws IOException {
        this.writer.write(text.getBytes());
    }

    @Override
    public void flush() throws IOException {
        this.writer.hflush();
    }

    @Override
    public void close() throws IOException {
        if (this.writer != null) {
            this.writer.flush();
            this.writer.close();
        }
    }
}
