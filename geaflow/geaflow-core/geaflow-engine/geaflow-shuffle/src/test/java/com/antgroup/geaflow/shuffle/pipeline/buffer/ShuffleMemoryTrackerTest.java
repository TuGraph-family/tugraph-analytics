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

package com.antgroup.geaflow.shuffle.pipeline.buffer;

import static com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys.CONTAINER_HEAP_SIZE_MB;
import static com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys.SHUFFLE_MEMORY_POOL_ENABLE;

import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ShuffleMemoryTrackerTest {

    @Test
    public void testRequireAndRelease() {
        Configuration configuration = new Configuration();
        configuration.put(ExecutionConfigKeys.CONTAINER_HEAP_SIZE_MB, String.valueOf(1024));
        ShuffleMemoryTracker tracker = new ShuffleMemoryTracker(configuration);

        byte[] bytes1 = new byte[100];
        HeapBuffer buffer1 = new HeapBuffer(bytes1, false);
        Assert.assertEquals(tracker.getUsedMemory(), 0);

        byte[] bytes2 = new byte[200];
        HeapBuffer buffer2 = new HeapBuffer(bytes2, tracker);
        Assert.assertEquals(tracker.getUsedMemory(), 200);

        buffer1.release();
        Assert.assertEquals(tracker.getUsedMemory(), 200);
        buffer2.release();
        Assert.assertEquals(tracker.getUsedMemory(), 0);
    }

    @Test
    public void testConcurrency() throws InterruptedException {
        Map<String, String> config = new HashMap<>();
        config.put(CONTAINER_HEAP_SIZE_MB.getKey(), "1000");
        ShuffleMemoryTracker tracker = new ShuffleMemoryTracker(new Configuration(config));

        Random random = new Random();
        CountDownLatch latch = new CountDownLatch(3);
        for (int i = 0; i < 3; i++)  {
            new Thread(new Runnable() {
                @Override
                public void run() {
                    for (int time = 0; time < 10; time++) {
                        int required = random.nextInt(1000);
                        boolean suc = tracker.requireMemory(required);
                        if (suc) {
                            tracker.releaseMemory(required);
                        } else {
                            System.out.println("not enough");
                        }
                    }
                    latch.countDown();
                }
            }).start();
        }
        latch.await();
        Assert.assertEquals(tracker.getUsedMemory(),  0);
    }

}
