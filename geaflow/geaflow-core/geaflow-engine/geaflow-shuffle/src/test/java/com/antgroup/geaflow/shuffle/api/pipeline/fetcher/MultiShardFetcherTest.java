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
import com.antgroup.geaflow.common.shuffle.ShuffleAddress;
import com.antgroup.geaflow.shuffle.api.pipeline.channel.AbstractInputChannel;
import com.antgroup.geaflow.shuffle.config.ShuffleConfig;
import com.antgroup.geaflow.shuffle.message.PipelineSliceMeta;
import com.antgroup.geaflow.shuffle.message.SliceId;
import com.antgroup.geaflow.shuffle.network.IConnectionManager;
import com.antgroup.geaflow.shuffle.network.netty.ConnectionManager;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.testng.Assert;
import org.testng.annotations.Test;

public class MultiShardFetcherTest {

    @Test
    public void testCreate() throws IOException {
        List<PipelineSliceMeta> inputSlices1 = new ArrayList<>();
        ShuffleConfig config = ShuffleConfig.getInstance(new Configuration());
        IConnectionManager connectionManager = new ConnectionManager(config);
        ShuffleAddress address = connectionManager.getShuffleAddress();
        PipelineSliceMeta slice1 = new PipelineSliceMeta(0, 0, -1, 0, address);
        inputSlices1.add(slice1);

        OneShardFetcher fetcher1 = new OneShardFetcher(1, "taskName", 0, inputSlices1, 0, connectionManager);
        Map<SliceId, AbstractInputChannel> channelMap = fetcher1.getInputChannels();
        Assert.assertEquals(channelMap.size(), 1);

        List<PipelineSliceMeta> inputSlices2 = new ArrayList<>();
        PipelineSliceMeta slice2 = new PipelineSliceMeta(0, 2, -1, 0, address);
        inputSlices2.add(slice2);

        OneShardFetcher fetcher2 = new OneShardFetcher(1, "taskName", 1, inputSlices2, 0, connectionManager);
        channelMap = fetcher2.getInputChannels();
        Assert.assertEquals(channelMap.size(), 1);

        MultiShardFetcher multiShardFetcher = new MultiShardFetcher(fetcher1, fetcher2);
        Assert.assertEquals(multiShardFetcher.getNumberOfInputChannels(), 2);

        ShardFetcher[] fetchers = multiShardFetcher.getShardFetchers();
        Assert.assertEquals(((OneShardFetcher)fetchers[0]).getFetcherIndex(), 0);
        Assert.assertEquals(((OneShardFetcher)fetchers[1]).getFetcherIndex(), 1);

        multiShardFetcher.close();
        connectionManager.close();
    }

}
