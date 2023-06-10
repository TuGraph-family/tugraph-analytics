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

package com.antgroup.geaflow.shuffle.serialize;

import com.antgroup.geaflow.model.record.impl.Record;
import com.antgroup.geaflow.shuffle.api.pipeline.buffer.HeapBuffer.HeapBufferBuilder;
import com.antgroup.geaflow.shuffle.api.pipeline.buffer.OutBuffer;
import com.antgroup.geaflow.shuffle.api.pipeline.buffer.OutBuffer.BufferBuilder;
import java.util.ArrayList;
import java.util.List;
import org.testng.Assert;
import org.testng.annotations.Test;

public class RecordSerializerTest {

    @Test
    public void test() {
        RecordSerializer serializer = new RecordSerializer();
        BufferBuilder outBuffer = new HeapBufferBuilder();
        List<Record> recordList = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            Record record = new Record(i);
            serializer.serialize(record, false, outBuffer);
            recordList.add(record);
        }
        OutBuffer buffer = outBuffer.build();
        MessageIterator deserializer = new MessageIterator(buffer);
        List<Record> list = new ArrayList<>();
        while (deserializer.hasNext()) {
            list.add((Record) deserializer.next());
        }
        Assert.assertEquals(recordList, list);
    }

}
