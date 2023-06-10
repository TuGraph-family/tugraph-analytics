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

import static com.antgroup.geaflow.dsl.connector.file.FileConstants.PREFIX_HDFS;

import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.exception.GeaflowRuntimeException;
import com.antgroup.geaflow.dsl.common.exception.GeaFlowDSLException;
import com.antgroup.geaflow.dsl.connector.api.Partition;
import com.antgroup.geaflow.dsl.connector.file.FileConnectorUtil;
import com.antgroup.geaflow.dsl.connector.file.source.FileTableSource.FileOffset;
import com.antgroup.geaflow.dsl.connector.file.source.FileTableSource.FileSplit;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HdfsFileReadHandler extends AbstractFileReadHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(HdfsFileReadHandler.class);

    protected Configuration conf;
    protected String path;

    protected Map<FileSplit, BufferedReader> readers;

    protected transient FileSystem fileSystem;

    @Override
    public void init(Configuration conf, String path) {
        if (path.startsWith(PREFIX_HDFS)) {
            this.path = path.substring(PREFIX_HDFS.length());
        } else {
            this.path = path;
        }
        this.readers = new HashMap<>();
        this.conf = conf;
        this.fileSystem = FileConnectorUtil.getHdfsFileSystem(conf);
        LOGGER.info("init hdfs file system. path: {}", path);
    }

    @Override
    public List<Partition> listPartitions() {
        try {
            if (fileSystem.isDirectory(new Path(path))) {
                RemoteIterator<LocatedFileStatus> files = fileSystem.listFiles(new Path(path),
                    true);
                List<Partition> partitions = new ArrayList<>();
                while (files.hasNext()) {
                    LocatedFileStatus f = files.next();
                    String relativePath = f.getPath().getName();
                    FileSplit partition = new FileSplit(path, relativePath);
                    LOGGER.info("fetch file partition: {}", partition);
                    partitions.add(partition);
                }
                return partitions;
            } else {
                String relativePath = new Path(path).getName();
                String directory = new Path(path).getParent().toUri().getPath();
                FileSplit partition = new FileSplit(directory, relativePath);
                LOGGER.info("fetch single file partition: {}", partition);
                return Collections.singletonList(partition);
            }
        } catch (Exception e) {
            LOGGER.error("Cannot get partitions for path: {}",  path);
            throw new GeaflowRuntimeException("Cannot get partitions with path: " + path, e);
        }
    }

    @Override
    protected BufferedReader getPartitionReader(FileSplit split, FileOffset offset) {
        try {
            if (!readers.containsKey(split)) {
                FSDataInputStream inputStream = fileSystem.open(new Path(split.getPath()));
                BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
                long skipOffset = reader.skip(offset.getOffset());
                assert skipOffset <= offset.getOffset();
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
