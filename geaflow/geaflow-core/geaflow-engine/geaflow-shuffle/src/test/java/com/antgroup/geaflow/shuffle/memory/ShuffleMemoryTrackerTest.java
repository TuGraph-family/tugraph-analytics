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

package com.antgroup.geaflow.shuffle.memory;

import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys;
import com.antgroup.geaflow.shuffle.api.pipeline.buffer.HeapBuffer;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ShuffleMemoryTrackerTest {

    @Test
    public void testRequireAndRelease() {
        Configuration configuration = new Configuration();
        configuration.put(ExecutionConfigKeys.CONTAINER_HEAP_SIZE_MB, String.valueOf(1024));
        ShuffleMemoryTracker.getInstance(configuration).dispose();
        ShuffleMemoryTracker tracker = ShuffleMemoryTracker.getInstance(configuration);

        byte[] bytes1 = new byte[100];
        HeapBuffer buffer1 = new HeapBuffer(bytes1);
        Assert.assertEquals(tracker.getUsedMemory(), 0);

        byte[] bytes2 = new byte[200];
        HeapBuffer buffer2 = new HeapBuffer(bytes2, true);
        Assert.assertEquals(tracker.getUsedMemory(), 200);

        buffer1.release();
        Assert.assertEquals(tracker.getUsedMemory(), 200);
        buffer2.release();
        Assert.assertEquals(tracker.getUsedMemory(), 0);
        tracker.dispose();
    }

}
