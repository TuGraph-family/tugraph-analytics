/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.antgroup.geaflow.shuffle.pipeline.fetcher;

import com.antgroup.geaflow.common.shuffle.ShuffleAddress;
import com.antgroup.geaflow.shuffle.message.ISliceMeta;
import com.antgroup.geaflow.shuffle.message.PipelineSliceMeta;
import com.antgroup.geaflow.shuffle.message.SliceId;
import com.antgroup.geaflow.shuffle.network.ConnectionId;
import com.antgroup.geaflow.shuffle.network.IConnectionManager;
import com.antgroup.geaflow.shuffle.pipeline.channel.AbstractInputChannel;
import com.antgroup.geaflow.shuffle.pipeline.channel.RemoteInputChannel;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MockedShardFetcher extends OneShardFetcher {
    private static final Logger LOGGER = LoggerFactory.getLogger(MockedShardFetcher.class);

    public MockedShardFetcher(int stageId, String taskName, int fetcherIndex,
                              List<? extends ISliceMeta> inputSlices, long startBatchId,
                              IConnectionManager connectionManager) {
        super(stageId, taskName, fetcherIndex, inputSlices, startBatchId,
            connectionManager);
    }

    @Override
    protected void buildInputChannels(int connectionId, List<? extends ISliceMeta> inputSlices,
                                      int initialBackoff, int maxBackoff, long startBatchId) {

        int localChannels = 0;
        for (int inputChannelIdx = 0; inputChannelIdx < numberOfInputChannels; inputChannelIdx++) {
            PipelineSliceMeta task = (PipelineSliceMeta) inputSlices.get(inputChannelIdx);
            ShuffleAddress address = task.getShuffleAddress();
            SliceId inputSlice = task.getSliceId();

            AbstractInputChannel inputChannel = new RemoteInputChannel(this, inputSlice,
                inputChannelIdx, new ConnectionId(address, connectionId), initialBackoff,
                maxBackoff, startBatchId, connectionManager);
            inputChannels.put(inputSlice, inputChannel);
        }
        LOGGER.info("create {} local channels in {} channels", localChannels, numberOfInputChannels);
    }

}
