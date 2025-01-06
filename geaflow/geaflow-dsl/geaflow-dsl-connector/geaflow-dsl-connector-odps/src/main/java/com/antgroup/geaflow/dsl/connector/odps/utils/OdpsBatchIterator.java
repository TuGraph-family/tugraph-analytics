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

package com.antgroup.geaflow.dsl.connector.odps.utils;

import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.RecordReader;
import com.antgroup.geaflow.common.exception.GeaflowRuntimeException;
import java.io.IOException;
import java.util.Iterator;
import java.util.Objects;

public class OdpsBatchIterator implements Iterator<OdpsRecordWithPartitionSpec> {

    private RecordReader reader;
    private long count;
    boolean finished = false;
    private final PartitionSpec spec;

    public OdpsBatchIterator(RecordReader reader, long count, PartitionSpec spec) {
        this.reader = Objects.requireNonNull(reader);
        this.spec = Objects.requireNonNull(spec);
        this.count = count;
        assert count >= 0;
    }

    @Override
    public boolean hasNext() {
        if (!finished && count > 0) {
            return true;
        } else {
            try {
                if (reader != null) {
                    reader.close();
                    reader = null;
                }
            } catch (IOException e) {
                throw new GeaflowRuntimeException("Error when close odps reader.");
            }
            return false;
        }
    }

    @Override
    public OdpsRecordWithPartitionSpec next() {
        Record record;
        try {
            record = reader.read();
        } catch (IOException e) {
            throw new GeaflowRuntimeException("Error when read odps.");
        }
        if (record == null) {
            finished = true;
        } else {
            count--;
        }
        return new OdpsRecordWithPartitionSpec(record, spec);
    }
}
