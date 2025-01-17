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

package com.antgroup.geaflow.shuffle.pipeline.slice;

import static com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys.SHUFFLE_SPILL_RECORDS;

import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys;
import com.antgroup.geaflow.shuffle.config.ShuffleConfig;
import com.antgroup.geaflow.shuffle.message.SliceId;
import com.antgroup.geaflow.shuffle.pipeline.buffer.HeapBuffer;
import com.antgroup.geaflow.shuffle.pipeline.buffer.PipeBuffer;
import com.antgroup.geaflow.shuffle.pipeline.buffer.PipeChannelBuffer;
import com.antgroup.geaflow.shuffle.pipeline.buffer.ShuffleMemoryTracker;
import java.io.IOException;
import java.util.UUID;
import org.testng.Assert;
import org.testng.annotations.Test;

public class SpillablePipelineSliceTest {

    @Test
    public void testAdd() throws IOException {
        Configuration config = new Configuration();
        config.put(SHUFFLE_SPILL_RECORDS, "50");
        config.put(ExecutionConfigKeys.CONTAINER_HEAP_SIZE_MB, String.valueOf(1024));

        long id = UUID.randomUUID().getLeastSignificantBits();
        SliceId sliceId = new SliceId(id, 0, 0, 0);
        ShuffleConfig shuffleConfig = new ShuffleConfig(config);
        ShuffleMemoryTracker memoryTracker = new ShuffleMemoryTracker(config);
        SpillablePipelineSlice slice = new SpillablePipelineSlice("test", sliceId, 2, shuffleConfig,
            memoryTracker);
        byte[] bytes1 = new byte[100];

        int bufferCount = 10000;
        for (int i = 0; i < bufferCount; i++) {
            HeapBuffer outBuffer = new HeapBuffer(bytes1, memoryTracker);
            PipeBuffer buffer = new PipeBuffer(outBuffer, 1, true);
            slice.add(buffer);
        }
        slice.flush();

        // Check repeatable reader.
        int consumedBufferCount = 0;
        PipelineSliceReader reader = slice.createSliceReader(1, () -> {});
        while (reader.hasNext()) {
            PipeChannelBuffer buffer = reader.next();
            if (buffer != null) {
                consumedBufferCount++;
            }
        }
        Assert.assertEquals(bufferCount, consumedBufferCount);
        Assert.assertFalse(slice.canRelease());

        // Check disposable reader.
        consumedBufferCount = 0;
        reader = slice.createSliceReader(1, () -> {});
        while (reader.hasNext()) {
            PipeChannelBuffer buffer = reader.next();
            if (buffer != null) {
                consumedBufferCount++;
            }
        }
        Assert.assertEquals(bufferCount, consumedBufferCount);
        Assert.assertTrue(slice.canRelease());
        slice.release();
    }

}
