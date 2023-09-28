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

package com.antgroup.geaflow.shuffle.api.reader;

import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.exception.GeaflowRuntimeException;
import com.antgroup.geaflow.shuffle.api.pipeline.buffer.PipeFetcherBuffer;
import com.antgroup.geaflow.shuffle.api.pipeline.fetcher.MultiShardFetcher;
import com.antgroup.geaflow.shuffle.api.pipeline.fetcher.OneShardFetcher;
import com.antgroup.geaflow.shuffle.api.pipeline.fetcher.ShardFetcher;
import com.antgroup.geaflow.shuffle.message.FetchRequest;
import com.antgroup.geaflow.shuffle.message.ISliceMeta;
import com.antgroup.geaflow.shuffle.message.PipelineBarrier;
import com.antgroup.geaflow.shuffle.message.PipelineEvent;
import com.antgroup.geaflow.shuffle.message.PipelineMessage;
import com.antgroup.geaflow.shuffle.message.PipelineSliceMeta;
import com.antgroup.geaflow.shuffle.message.SliceId;
import com.antgroup.geaflow.shuffle.network.IConnectionManager;
import com.antgroup.geaflow.shuffle.serialize.IMessageIterator;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PipelineFetcher extends AbstractFetcher<PipelineSliceMeta> {

    private static final Logger LOGGER = LoggerFactory.getLogger(PipelineFetcher.class);
    private static final int INFINITE_BATCHES = -1;

    private IConnectionManager connectionManager;
    private Set<Integer> inputEdgeSet;
    private ShardFetcher inputFetcher;
    private volatile boolean isRunning;

    public PipelineFetcher() {
    }

    @Override
    public void setup(IConnectionManager connectionManager, Configuration config) {
        super.setup(connectionManager, config);
        this.connectionManager = connectionManager;
        this.inputEdgeSet = new HashSet<>();
        this.isRunning = true;
    }

    @Override
    public void init(FetchContext<PipelineSliceMeta> fetchContext) {
        super.init(fetchContext);

        ShardFetcher previous = inputFetcher;
        inputFetcher = getInputFetcher(fetchContext);
        try {
            inputFetcher.requestSlices(targetBatchId);
            if (previous != null && previous != inputFetcher) {
                // Close previous after new connections created to reuse the tcp channels.
                previous.close();
            }
        } catch (IOException e) {
            LOGGER.error(e.getMessage(), e.getCause());
            throw new GeaflowRuntimeException("fetch error", e);
        }
    }

    @Override
    public PipelineEvent next() {
        long startTime = System.currentTimeMillis();
        try {
            Optional<PipeFetcherBuffer> next = inputFetcher.getNext();
            if (next.isPresent()) {
                PipeFetcherBuffer buffer = next.get();
                if (buffer.isBarrier()) {
                    if (buffer.getBatchId() == targetBatchId || buffer.isFinish()) {
                        processedNum++;
                    }

                    SliceId sliceId = buffer.getSliceId();
                    PipelineBarrier barrier = new PipelineBarrier(buffer.getBatchId(),
                        sliceId.getEdgeId(), sliceId.getShardIndex(), sliceId.getSliceIndex(),
                        buffer.getBatchCount());
                    barrier.setFinish(buffer.isFinish());
                    return barrier;
                } else {
                    int edgeId = buffer.getSliceId().getEdgeId();
                    this.readMetrics.increaseDecodeBytes(buffer.getBufferSize());
                    IMessageIterator<?> msgIterator = this.getMessageIterator(edgeId, buffer.getBuffer());
                    return new PipelineMessage<>(buffer.getBatchId(), buffer.getStreamName(), msgIterator);
                }
            } else {
                if (!isRunning) {
                    return null;
                }
                throw new GeaflowRuntimeException(taskName + " get null");
            }
        } catch (IOException | InterruptedException e) {
            LOGGER.error(e.getMessage(), e.getCause());
            throw new GeaflowRuntimeException(e);
        } finally {
            readMetrics.incFetchWaitMs(System.currentTimeMillis() - startTime);
        }
    }

    @Override
    public boolean hasNext() {
        return !inputFetcher.isFinished() && (totalSliceNum == INFINITE_BATCHES
            || processedNum < totalSliceNum);
    }

    @Override
    public void close() {
        isRunning = false;
        if (inputFetcher != null) {
            inputFetcher.close();
        }
    }

    private ShardFetcher getInputFetcher(FetchContext fetchContext) {
        ShardFetcher shardFetcher = inputFetcher;
        Map<Integer, List<ISliceMeta>> inputSlices = fetchContext.getInputSliceMap();
        if (inputSlices == null) {
            inputSlices = fetchContext.getRequest().getInputSlices();
        }
        if (inputSlices != null && !inputSlices.isEmpty()) {
            shardFetcher = createShardFetcher(fetchContext.getRequest(), inputSlices);
        }
        return shardFetcher;
    }

    private ShardFetcher createShardFetcher(FetchRequest req, Map<Integer,
        List<ISliceMeta>> inputSlices) {
        Set<Integer> edgeSet = inputSlices.keySet();
        if (checkInputUnchanged(inputSlices)) {
            return inputFetcher;
        } else {
            inputEdgeSet.clear();
            inputEdgeSet.addAll(edgeSet);
        }

        int channels = 0;
        int fetcherIndex = 0;
        long batchId = req.getTargetBatchId();

        Map<Integer, String> inputStreamMap = req.getInputStreamMap();
        List<OneShardFetcher> fetchers = new ArrayList<>(inputSlices.size());
        for (Map.Entry<Integer, List<ISliceMeta>> entry : inputSlices.entrySet()) {
            int edgeId = entry.getKey();
            String streamName = inputStreamMap.get(edgeId);
            OneShardFetcher inputFetcher = new OneShardFetcher(req.getVertexId(), taskName,
                fetcherIndex, edgeId, streamName, entry.getValue(), batchId, connectionManager);
            fetchers.add(inputFetcher);
            fetcherIndex++;
            channels += entry.getValue().size();
        }
        totalSliceNum = batchId < 0 ? INFINITE_BATCHES : channels;

        if (fetchers.size() == 1) {
            return fetchers.get(0);
        } else {
            return new MultiShardFetcher(fetchers.toArray(new OneShardFetcher[0]));
        }
    }

    private boolean checkInputUnchanged(Map<Integer, List<ISliceMeta>> inputSlices) {
        Set<Integer> edgeSet = inputSlices.keySet();
        if (edgeSet.size() == inputEdgeSet.size()) {
            return inputEdgeSet.containsAll(edgeSet);
        }
        return false;
    }

}
