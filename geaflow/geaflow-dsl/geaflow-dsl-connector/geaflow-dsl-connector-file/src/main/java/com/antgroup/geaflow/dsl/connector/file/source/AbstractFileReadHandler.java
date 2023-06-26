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

import com.antgroup.geaflow.dsl.common.util.Windows;
import com.antgroup.geaflow.dsl.connector.api.FetchData;
import com.antgroup.geaflow.dsl.connector.file.source.FileTableSource.FileOffset;
import com.antgroup.geaflow.dsl.connector.file.source.FileTableSource.FileSplit;
import java.io.BufferedReader;
import java.io.IOException;
import java.util.Collections;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractFileReadHandler implements FileReadHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractFileReadHandler.class);

    protected abstract BufferedReader getPartitionReader(FileSplit split, FileOffset offset);

    @Override
    public FetchData<String> readPartition(FileSplit split, FileOffset offset, long size) throws IOException {
        BufferedReader reader = getPartitionReader(split, offset);

        String readContent;
        long nextOffset;
        boolean isFinished;
        if (size == Windows.SIZE_OF_ALL_WINDOW) { // read all data from file
            StringBuilder content = new StringBuilder();
            char[] buffer = new char[1024];
            int readSize;
            int totalRead = 0;
            while ((readSize = reader.read(buffer)) != -1) {
                content.append(buffer, 0, readSize);
                totalRead += readSize;
            }
            readContent = content.toString();
            nextOffset = totalRead;
            isFinished = true;
        } else {
            nextOffset = offset.getOffset();
            StringBuilder lines = new StringBuilder();
            int i;
            for (i = 0; i < size; i++) {
                String line = reader.readLine();
                if (line == null ) {
                    if (lines.length() > 0) {
                        lines.deleteCharAt(lines.length() - 1);
                    }
                    break;
                }
                if (i != size - 1) {
                    line = line + "\n";
                }
                lines.append(line);
                nextOffset += line.length();
            }
            readContent = lines.toString();
            isFinished = i < size;
        }

        return new FetchData<>(Collections.singletonList(readContent), new FileOffset(nextOffset), isFinished);
    }
}
