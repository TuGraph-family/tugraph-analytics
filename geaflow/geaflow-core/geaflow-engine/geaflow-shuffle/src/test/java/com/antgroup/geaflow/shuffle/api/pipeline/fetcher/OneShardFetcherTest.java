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

package com.antgroup.geaflow.shuffle.api.pipeline.fetcher;

import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys;
import com.antgroup.geaflow.common.shuffle.ShuffleAddress;
import com.antgroup.geaflow.shuffle.api.pipeline.buffer.PipeBuffer;
import com.antgroup.geaflow.shuffle.api.pipeline.buffer.PipeFetcherBuffer;
import com.antgroup.geaflow.shuffle.api.pipeline.buffer.PipelineShard;
import com.antgroup.geaflow.shuffle.api.pipeline.buffer.PipelineSlice;
import com.antgroup.geaflow.shuffle.api.pipeline.channel.AbstractInputChannel;
import com.antgroup.geaflow.shuffle.api.pipeline.channel.LocalInputChannel;
import com.antgroup.geaflow.shuffle.api.pipeline.channel.RemoteInputChannel;
import com.antgroup.geaflow.shuffle.config.ShuffleConfig;
import com.antgroup.geaflow.shuffle.memory.ShuffleDataManager;
import com.antgroup.geaflow.shuffle.memory.ShuffleMemoryTracker;
import com.antgroup.geaflow.shuffle.message.PipelineSliceMeta;
import com.antgroup.geaflow.shuffle.message.SliceId;
import com.antgroup.geaflow.shuffle.network.IConnectionManager;
import com.antgroup.geaflow.shuffle.network.netty.ConnectionManager;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

public class OneShardFetcherTest {

    private IConnectionManager connectionManager;

    @BeforeTest
    public void setup() {
        Configuration configuration = new Configuration();
        configuration.put(ExecutionConfigKeys.JOB_APP_NAME, "default");
        configuration.put(ExecutionConfigKeys.CONTAINER_HEAP_SIZE_MB, String.valueOf(1024));
        ShuffleConfig config = ShuffleConfig.getInstance(configuration);
        connectionManager = new ConnectionManager(config);
    }

    @AfterTest
    public void destroy() throws IOException {
        connectionManager.close();
    }

    @Test
    public void testCreate() {
        ShuffleMemoryTracker.getInstance(ShuffleConfig.getInstance().getConfig());
        List<PipelineSliceMeta> inputSlices = new ArrayList<>();
        ShuffleAddress address = connectionManager.getShuffleAddress();
        PipelineSliceMeta slice1 = new PipelineSliceMeta(0, 0, -1, 0, address);
        PipelineSliceMeta slice2 = new PipelineSliceMeta(0, 2, -1, 0, address);
        inputSlices.add(slice1);
        inputSlices.add(slice2);

        OneShardFetcher fetcher = new OneShardFetcher(1, "taskName", 0, inputSlices, 0,
            connectionManager);
        Map<SliceId, AbstractInputChannel> channelMap = fetcher.getInputChannels();
        Assert.assertEquals(channelMap.size(), 2);

        Set<SliceId> expectedSlices = new HashSet<>();
        expectedSlices.add(new SliceId(-1, 0, 0, 0));
        expectedSlices.add(new SliceId(-1, 0, 0, 2));

        Set<Integer> expectedChannelIndices = new HashSet<>();
        expectedChannelIndices.add(0);
        expectedChannelIndices.add(1);

        Set<Integer> channelIds = new HashSet<>();
        for (Map.Entry<SliceId, AbstractInputChannel> entry : channelMap.entrySet()) {
            Assert.assertTrue(expectedSlices.remove(entry.getKey()));
            AbstractInputChannel channel = entry.getValue();
            channelIds.add(channel.getChannelIndex());
            Assert.assertTrue(channel instanceof LocalInputChannel);
        }
        Assert.assertEquals(expectedChannelIndices, channelIds);
    }

    @Test
    public void testRemoteFetch() throws IOException, InterruptedException {
        ShuffleMemoryTracker.getInstance(ShuffleConfig.getInstance().getConfig());
        List<PipelineSliceMeta> inputSlices = new ArrayList<>();
        ShuffleAddress address = connectionManager.getShuffleAddress();
        SliceId sliceId = new SliceId(-1, 0, 0, 0);
        PipelineSliceMeta slice1 = new PipelineSliceMeta(sliceId, 1, address);
        inputSlices.add(slice1);

        OneShardFetcher fetcher = new MockedShardFetcher(1, "taskName", 0, inputSlices, 0,
            connectionManager);

        PipeBuffer pipeBuffer = new PipeBuffer("hello".getBytes(), 1, true);
        PipeBuffer pipeBuffer2 = new PipeBuffer("barrier".getBytes(), 1, false);
        PipeBuffer pipeBuffer3 = new PipeBuffer("hello".getBytes(), 2, true);
        PipeBuffer pipeBuffer4 = new PipeBuffer("barrier".getBytes(), 2, false);
        PipelineSlice slice = new PipelineSlice("task", sliceId);
        slice.add(pipeBuffer);
        slice.add(pipeBuffer2);
        slice.add(pipeBuffer3);
        slice.add(pipeBuffer4);
        Assert.assertEquals(slice.getNumberOfBuffers(), 4);

        PipelineShard pipeShard = new PipelineShard(new PipelineSlice[]{slice});
        ShuffleDataManager shuffleDataManager = ShuffleDataManager.getInstance();
        shuffleDataManager.register(sliceId.getWriterId(), pipeShard);

        List<Long> batchList = Arrays.asList(1L, 2L);
        List<String> result = new ArrayList<>();
        for (long batchId : batchList) {
            fetcher.requestSlices(batchId);
            while (!fetcher.isFinished()) {
                Optional<PipeFetcherBuffer> bufferOptional = fetcher.getNext();
                if (bufferOptional.isPresent()) {
                    PipeFetcherBuffer buffer = bufferOptional.get();
                    String value = String.valueOf(buffer.getBuffer());
                    result.add(value);
                    if (buffer.isBarrier()) {
                        break;
                    }
                }
            }
        }

        Assert.assertEquals(result.size(), 4);
        Assert.assertFalse(slice.getSliceReader().hasNext());
        Assert.assertEquals(slice.getNumberOfBuffers(), 0);

        fetcher.close();
        for (AbstractInputChannel channel : fetcher.getInputChannels().values()) {
            if (channel instanceof RemoteInputChannel) {
                RemoteInputChannel remoteChannel = (RemoteInputChannel) channel;
                Assert.assertTrue(remoteChannel.getSliceRequestClient().disposeIfNotUsed());
            }
        }

    }

}
