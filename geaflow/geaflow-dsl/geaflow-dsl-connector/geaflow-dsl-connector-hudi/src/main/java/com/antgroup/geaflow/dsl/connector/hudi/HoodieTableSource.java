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

package com.antgroup.geaflow.dsl.connector.hudi;

import com.antgroup.geaflow.api.context.RuntimeContext;
import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.config.keys.ConnectorConfigKeys;
import com.antgroup.geaflow.dsl.common.data.Row;
import com.antgroup.geaflow.dsl.common.exception.GeaFlowDSLException;
import com.antgroup.geaflow.dsl.common.types.TableSchema;
import com.antgroup.geaflow.dsl.common.util.Windows;
import com.antgroup.geaflow.dsl.connector.api.FetchData;
import com.antgroup.geaflow.dsl.connector.api.Offset;
import com.antgroup.geaflow.dsl.connector.api.Partition;
import com.antgroup.geaflow.dsl.connector.api.TableSource;
import com.antgroup.geaflow.dsl.connector.api.serde.TableDeserializer;
import com.antgroup.geaflow.dsl.connector.file.FileConnectorUtil;
import com.antgroup.geaflow.dsl.connector.file.source.FileTableSource.FileOffset;
import com.antgroup.geaflow.dsl.connector.file.source.FileTableSource.FileSplit;
import com.antgroup.geaflow.dsl.connector.file.source.format.ParquetFormat;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.BaseHoodieTableFileIndex.PartitionPath;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HoodieTableSource implements TableSource {

    private static final Logger LOGGER = LoggerFactory.getLogger(HoodieTableSource.class);

    private Configuration tableConf;

    private TableSchema tableSchema;

    private GeaFlowHoodieTableFileIndex fileIndex;

    private Path basePath;

    @Override
    public void init(Configuration tableConf, TableSchema tableSchema) {
        this.tableConf = tableConf;
        this.tableSchema = tableSchema;
    }

    @Override
    public void open(RuntimeContext context) {
        this.fileIndex = GeaFlowHoodieTableFileIndex.create(context, tableConf);
        this.basePath = new Path(tableConf.getString(ConnectorConfigKeys.GEAFLOW_DSL_FILE_PATH));
        FileSystem fileSystem = FileConnectorUtil.getHdfsFileSystem(tableConf);
        this.basePath = fileSystem.makeQualified(basePath);
        LOGGER.info("open hudi table source: {}", basePath);
    }

    @Override
    public List<Partition> listPartitions() {
        List<PartitionPath> paths = fileIndex.getAllQueryPartitionPaths();
        List<Partition> fileSplits = new ArrayList<>();
        for (PartitionPath path : paths) {
            fileSplits.add(new FileSplit(basePath.toString(), path.getPath()));
        }
        return fileSplits;
    }

    @Override
    public <IN> TableDeserializer<IN> getDeserializer(Configuration conf) {
        // no op here as hoodie table source return row already, deserializer is no need.
        return null;
    }

    @Override
    public <T> FetchData<T> fetch(Partition partition, Optional<Offset> startOffset, long windowSize)
        throws IOException {
        if (windowSize == Windows.SIZE_OF_ALL_WINDOW) {
            FileSplit split = (FileSplit) partition;
            ParquetFormat format = new ParquetFormat();
            format.init(tableConf, tableSchema, split);
            Iterator<Row> iterator = format.batchRead();
            Offset nextOffset = new FileOffset(-1L);
            format.close();
            return (FetchData<T>) FetchData.createBatchFetch(iterator, nextOffset);
        }
        throw new GeaFlowDSLException("Stream read for hudi is no support currently");
    }

    @Override
    public void close() {

    }
}
