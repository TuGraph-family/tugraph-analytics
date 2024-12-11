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

package com.antgroup.geaflow.shuffle.api.writer;

import static com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys.CONTAINER_HEAP_SIZE_MB;
import static com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys.SHUFFLE_SPILL_RECORDS;

import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.shuffle.ShuffleAddress;
import com.antgroup.geaflow.shuffle.message.Shard;
import com.antgroup.geaflow.shuffle.network.IConnectionManager;
import com.antgroup.geaflow.shuffle.pipeline.slice.SliceManager;
import com.antgroup.geaflow.shuffle.service.ShuffleManager;
import java.io.IOException;
import java.util.Optional;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

public class SpillableShardWriterTest {

    @Test
    public void testEmit() throws IOException {
        IConnectionManager connectionManager = Mockito.mock(IConnectionManager.class);
        Mockito.when(connectionManager.getShuffleAddress()).thenReturn(new ShuffleAddress(
            "localhost", 1));

        Configuration config = new Configuration();
        config.put(SHUFFLE_SPILL_RECORDS, "50");
        config.put(CONTAINER_HEAP_SIZE_MB, "1");
        ShuffleManager.init(config);

        SpillableShardWriter shardWriter = new SpillableShardWriter(connectionManager.getShuffleAddress());
        WriterContext writerContext = new WriterContext(2, "name");

        writerContext.setConfig(config);
        writerContext.setChannelNum(1);
        shardWriter.init(writerContext);
        int[] channels = new int[]{0};

        for (int i = 0; i < 10000; i++) {
            shardWriter.emit(0, "hello, testing spillable writer", false, channels);
        }
        Optional<Shard> optional = shardWriter.finish(0);
        Shard shard = optional.get();
        Assert.assertNotNull(shard);
        Assert.assertEquals(shard.getSlices().size(), 1);

        SliceManager.getInstance().release(2);
    }

}