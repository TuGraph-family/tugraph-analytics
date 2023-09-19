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

import com.antgroup.geaflow.api.context.RuntimeContext;
import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.config.keys.ConnectorConfigKeys;
import com.antgroup.geaflow.dsl.common.exception.GeaFlowDSLException;
import com.antgroup.geaflow.dsl.common.types.TableSchema;
import com.antgroup.geaflow.dsl.connector.api.FetchData;
import com.antgroup.geaflow.dsl.connector.api.Offset;
import com.antgroup.geaflow.dsl.connector.api.Partition;
import com.antgroup.geaflow.dsl.connector.api.TableSource;
import com.antgroup.geaflow.dsl.connector.api.serde.TableDeserializer;
import com.antgroup.geaflow.dsl.connector.file.FileConnectorUtil;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.regex.Pattern;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileTableSource implements TableSource {

    private static final Logger LOGGER = LoggerFactory.getLogger(FileTableSource.class);

    private String path;

    private Configuration tableConf;

    private TableSchema tableSchema;

    private String nameFilterRegex;

    private transient FileReadHandler fileReadHandler;

    @Override
    public void init(Configuration tableConf, TableSchema tableSchema) {
        this.path = tableConf.getString(ConnectorConfigKeys.GEAFLOW_DSL_FILE_PATH);
        this.tableConf = tableConf;
        this.tableSchema = tableSchema;
        this.nameFilterRegex = tableConf.getString(ConnectorConfigKeys.GEAFLOW_DSL_FILE_NAME_REGEX);
        LOGGER.info("init table source with tableConf: {}", tableConf);
    }

    @Override
    public void open(RuntimeContext context) {
        this.fileReadHandler = FileReadHandlers.from(path);
        try {
            this.fileReadHandler.init(tableConf, tableSchema, path);
        } catch (IOException e) {
            throw new GeaFlowDSLException("Error in open file source", e);
        }
        LOGGER.info("open table source on path: {}", path);
    }

    @Override
    public List<Partition> listPartitions() {
        List<Partition> allPartitions = fileReadHandler.listPartitions();
        if (StringUtils.isNotEmpty(this.nameFilterRegex)) {
            List<Partition> filterPartitions = new ArrayList<>();
            for (Partition partition : allPartitions) {
                if (!partition.getName().startsWith(".")
                    && Pattern.matches(this.nameFilterRegex, partition.getName())) {
                    filterPartitions.add(partition);
                }
            }
            return filterPartitions;
        }
        return allPartitions;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <IN> TableDeserializer<IN> getDeserializer(Configuration conf) {
        return fileReadHandler.getDeserializer();
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> FetchData<T> fetch(Partition partition, Optional<Offset> startOffset,
                                  long windowSize) throws IOException {
        FileOffset offset = startOffset.map(value -> (FileOffset) value).orElseGet(() -> new FileOffset(0L));
        return fileReadHandler.readPartition((FileSplit) partition, offset, (int) windowSize);
    }

    @Override
    public void close() {
        try {
            fileReadHandler.close();
        } catch (IOException e) {
            throw new GeaFlowDSLException("Error in close file read handler", e);
        }
    }

    public static class FileSplit implements Partition {

        private final String baseDir;

        private final String relativePath;

        public FileSplit(String baseDir, String relativePath) {
            this.baseDir = baseDir;
            this.relativePath = relativePath;
        }

        public FileSplit(String file) {
            int index = file.lastIndexOf('/');
            if (index == -1) {
                throw new GeaFlowDSLException("Illegal file path: '{}', should be a full path.", file);
            }
            this.baseDir = file.substring(0, index);
            this.relativePath = file.substring(index + 1);
        }

        @Override
        public String getName() {
            return relativePath;
        }

        public String getPath() {
            if (baseDir.endsWith("/")) {
                return baseDir + relativePath;
            }
            return baseDir + "/" + relativePath;
        }

        @Override
        public int hashCode() {
            return Objects.hash(baseDir, relativePath);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof FileSplit)) {
                return false;
            }
            FileSplit that = (FileSplit) o;
            return Objects.equals(baseDir, that.baseDir) && Objects.equals(relativePath, that.relativePath);
        }

        @Override
        public String toString() {
            return "FileSplit(path=" + getPath() + ")";
        }

        public InputStream openStream(Configuration conf) throws IOException {
            FileSystem fs = FileConnectorUtil.getHdfsFileSystem(conf);
            Path path = new Path(baseDir, relativePath);
            return fs.open(path);
        }
    }

    public static class FileOffset implements Offset {

        private final long offset;

        public FileOffset(long offset) {
            this.offset = offset;
        }

        @Override
        public String humanReadable() {
            return String.valueOf(offset);
        }

        @Override
        public long getOffset() {
            return offset;
        }

        @Override
        public boolean isTimestamp() {
            return false;
        }
    }
}
