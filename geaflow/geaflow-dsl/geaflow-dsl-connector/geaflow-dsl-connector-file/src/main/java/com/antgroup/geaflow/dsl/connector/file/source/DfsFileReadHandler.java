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

import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.exception.GeaflowRuntimeException;
import com.antgroup.geaflow.dsl.common.types.TableSchema;
import com.antgroup.geaflow.dsl.connector.api.Partition;
import com.antgroup.geaflow.dsl.connector.file.FileConnectorUtil;
import com.antgroup.geaflow.dsl.connector.file.source.FileTableSource.FileSplit;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DfsFileReadHandler extends AbstractFileReadHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(DfsFileReadHandler.class);

    protected Configuration tableConf;
    protected Path path;

    @Override
    public void init(Configuration tableConf, TableSchema tableSchema, String path) throws IOException {
        super.init(tableConf, tableSchema, path);
        org.apache.hadoop.conf.Configuration hadoopConf = FileConnectorUtil.toHadoopConf(tableConf);
        this.path = new Path(path);
        this.path = this.path.getFileSystem(hadoopConf).makeQualified(this.path);
        this.fileFormats = new HashMap<>();
        this.tableConf = tableConf;
        LOGGER.info("init hdfs file system. path: {}", path);
    }

    @Override
    public List<Partition> listPartitions() {
        try {
            FileSystem fileSystem = FileConnectorUtil.getHdfsFileSystem(tableConf);
            if (fileSystem.isDirectory(path)) {
                RemoteIterator<LocatedFileStatus> files = fileSystem.listFiles(path, true);
                List<Partition> partitions = new ArrayList<>();
                while (files.hasNext()) {
                    LocatedFileStatus f = files.next();
                    String relativePath = f.getPath().getName();
                    FileSplit partition = new FileSplit(path.toString(), relativePath);
                    partitions.add(partition);
                }
                return partitions;
            } else {
                String relativePath = path.getName();
                String directory = path.getParent().toUri().getPath();
                FileSplit partition = new FileSplit(directory, relativePath);
                return Collections.singletonList(partition);
            }
        } catch (Exception e) {
            throw new GeaflowRuntimeException("Cannot get partitions with path: " + path, e);
        }
    }
}
