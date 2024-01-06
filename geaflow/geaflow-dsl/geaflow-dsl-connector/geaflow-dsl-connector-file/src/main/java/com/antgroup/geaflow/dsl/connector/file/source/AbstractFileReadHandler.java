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
import com.antgroup.geaflow.common.config.keys.ConnectorConfigKeys;
import com.antgroup.geaflow.dsl.common.exception.GeaFlowDSLException;
import com.antgroup.geaflow.dsl.common.types.TableSchema;
import com.antgroup.geaflow.dsl.common.util.Windows;
import com.antgroup.geaflow.dsl.connector.api.FetchData;
import com.antgroup.geaflow.dsl.connector.api.serde.TableDeserializer;
import com.antgroup.geaflow.dsl.connector.file.source.FileTableSource.FileOffset;
import com.antgroup.geaflow.dsl.connector.file.source.FileTableSource.FileSplit;
import com.antgroup.geaflow.dsl.connector.file.source.format.FileFormat;
import com.antgroup.geaflow.dsl.connector.file.source.format.FileFormats;
import com.antgroup.geaflow.dsl.connector.file.source.format.StreamFormat;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public abstract class AbstractFileReadHandler implements FileReadHandler {

    private String formatName;
    private Configuration tableConf;
    private TableSchema tableSchema;

    protected Map<FileSplit, FileFormat> fileFormats = new HashMap<>();

    @Override
    public void init(Configuration tableConf, TableSchema tableSchema, String path) throws IOException {
        this.formatName = tableConf.getString(ConnectorConfigKeys.GEAFLOW_DSL_FILE_FORMAT);
        this.tableConf = tableConf;
        this.tableSchema = tableSchema;
    }

    @Override
    public <T> FetchData<T> readPartition(FileSplit split, FileOffset offset, int windowSize) throws IOException {
        FileFormat<T> format = getFileFormat(split, offset);
        if (windowSize == Windows.SIZE_OF_ALL_WINDOW) { // read all data from file
            Iterator<T> iterator = format.batchRead();
            return FetchData.createBatchFetch(iterator, new FileOffset(-1));
        } else {
            if (!(format instanceof StreamFormat)) {
                throw new GeaFlowDSLException("Format '{}' is not a stream format, the window size should be -1",
                    formatName);
            }
            StreamFormat<T> streamFormat = (StreamFormat<T>) format;
            return streamFormat.streamRead(offset, windowSize);
        }
    }

    private <T> FileFormat<T> getFileFormat(FileSplit split, FileOffset offset) throws IOException {
        if (!fileFormats.containsKey(split)) {
            FileFormat<T> fileFormat = FileFormats.loadFileFormat(formatName);
            fileFormat.init(tableConf, tableSchema, split);
            // skip pre offset for stream read.
            if (fileFormat instanceof StreamFormat) {
                ((StreamFormat) fileFormat).skip(offset.getOffset());
            }
            fileFormats.put(split, fileFormat);
        }
        return fileFormats.get(split);
    }

    @Override
    public void close() throws IOException {
        for (FileFormat format : fileFormats.values()) {
            format.close();
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> TableDeserializer<T> getDeserializer() {
        return (TableDeserializer<T>) FileFormats.loadFileFormat(formatName).getDeserializer();
    }
}
