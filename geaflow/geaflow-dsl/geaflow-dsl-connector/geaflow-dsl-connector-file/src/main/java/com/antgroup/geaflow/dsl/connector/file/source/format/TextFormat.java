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

package com.antgroup.geaflow.dsl.connector.file.source.format;

import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.dsl.common.exception.GeaFlowDSLException;
import com.antgroup.geaflow.dsl.common.types.TableSchema;
import com.antgroup.geaflow.dsl.connector.api.FetchData;
import com.antgroup.geaflow.dsl.connector.api.serde.TableDeserializer;
import com.antgroup.geaflow.dsl.connector.api.serde.impl.TextDeserializer;
import com.antgroup.geaflow.dsl.connector.file.source.FileTableSource.FileOffset;
import com.antgroup.geaflow.dsl.connector.file.source.FileTableSource.FileSplit;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class TextFormat implements FileFormat<String>, StreamFormat<String> {

    private BufferedReader reader;

    @Override
    public String getFormat() {
        return "txt";
    }

    @Override
    public void init(Configuration tableConf, TableSchema tableSchema, FileSplit split) throws IOException {
        this.reader = new BufferedReader(new InputStreamReader(split.openStream(tableConf)));
    }

    @Override
    public Iterator<String> batchRead() {
        return new Iterator<String>() {

            private String current = null;

            @Override
            public boolean hasNext() {
                try {
                    if (current == null) {
                        current = reader.readLine();
                    }
                    return current != null;
                } catch (IOException e) {
                    throw new GeaFlowDSLException("Error in read", e);
                }
            }

            @Override
            public String next() {
                String next = current;
                current = null;
                return next;
            }
        };
    }

    @Override
    public FetchData<String> streamRead(FileOffset offset, int windowSize) throws IOException {
        List<String> readContents = new ArrayList<>(windowSize);
        long nextOffset = offset.getOffset();
        int i;
        for (i = 0; i < windowSize; i++) {
            String line = reader.readLine();
            if (line == null) {
                break;
            }
            readContents.add(line);
            nextOffset += line.length() + 1;
        }
        boolean isFinished = i < windowSize;
        return FetchData.createStreamFetch(readContents, new FileOffset(nextOffset), isFinished);
    }

    @Override
    public void close() throws IOException {
        if (reader != null) {
            reader.close();
        }
    }

    @Override
    public TableDeserializer<String> getDeserializer() {
        return new TextDeserializer();
    }

    @Override
    public void skip(long n) throws IOException {
        reader.skip(n);
    }
}
