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

import com.antgroup.geaflow.shuffle.api.pipeline.buffer.PipelineShard;
import com.antgroup.geaflow.shuffle.api.pipeline.buffer.PipelineSlice;
import com.antgroup.geaflow.shuffle.api.pipeline.fetcher.PipelineSliceListener;
import com.antgroup.geaflow.shuffle.api.pipeline.fetcher.PipelineSliceReader;
import com.antgroup.geaflow.shuffle.message.SliceId;
import com.antgroup.geaflow.shuffle.message.WriterId;
import com.antgroup.geaflow.shuffle.util.SliceNotFoundException;
import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ShuffleDataManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(ShuffleDataManager.class);
    private static ShuffleDataManager INSTANCE;

    private final Map<WriterId, PipelineShard> entries = new ConcurrentHashMap<>();
    private final Set<Long> pipelineSet = new HashSet<>();

    public static synchronized ShuffleDataManager getInstance() {
        if (INSTANCE == null) {
            INSTANCE = new ShuffleDataManager();
        }
        return INSTANCE;
    }

    public void register(WriterId writerId, PipelineShard pipeShard) {
        LOGGER.info("{} register {} {} slices", pipeShard.getTaskName(), writerId,
            pipeShard.getSliceNum());
        PipelineShard previousShard = entries.put(writerId, pipeShard);
        if (previousShard != null) {
            if (pipeShard.hasData()) {
                throw new IllegalStateException("already registered:" + writerId);
            }
        }
        synchronized (pipelineSet) {
            pipelineSet.add(writerId.getPipelineId());
        }
    }

    public PipelineShard getShard(WriterId writerID) {
        return entries.get(writerID);
    }

    public PipelineSlice getSlice(SliceId sliceId) {
        PipelineShard shard = entries.get(sliceId.getWriterId());
        if (shard == null) {
            return null;
        }
        return shard.getSlice(sliceId.getSliceIndex());
    }

    public PipelineSliceReader createSliceReader(SliceId sliceId, long startBatchId,
                                                 PipelineSliceListener listener) throws IOException {
        PipelineSlice slice = getSlice(sliceId);
        if (slice == null) {
            throw new SliceNotFoundException(sliceId);
        }
        return slice.createSliceReader(startBatchId, listener);
    }

    public int getBlockCount() {
        int cacheSize = 0;
        for (PipelineShard block : entries.values()) {
            cacheSize += block.getBufferCount();
        }
        return cacheSize;
    }

    public void release(SliceId sliceId) {
        WriterId writerID = sliceId.getWriterId();
        PipelineShard shard = entries.get(writerID);
        if (shard != null) {
            shard.release(sliceId.getSliceIndex());
            if (shard.disposedIfNeed()) {
                shard.release();
                entries.remove(writerID);
                LOGGER.info("remove {} {}", shard.getTaskName(), writerID);
            }
        }
    }

    public void release(long pipelineId) {
        synchronized (pipelineSet) {
            if (pipelineSet.contains(pipelineId)) {
                int totalShards = 0;
                Iterator<Map.Entry<WriterId, PipelineShard>> shardIterator = entries.entrySet().iterator();
                while (shardIterator.hasNext()) {
                    Map.Entry<WriterId, PipelineShard> entry = shardIterator.next();
                    if (entry.getKey().getPipelineId() == pipelineId) {
                        entry.getValue().release();
                        shardIterator.remove();
                        totalShards++;
                    }
                }
                LOGGER.info("cleanup {} shuffle shards: {}", pipelineId, totalShards);
            }
            pipelineSet.remove(pipelineId);
        }
    }

    public synchronized void releaseAll() {
        int totalShards = 0;
        synchronized (entries) {
            for (PipelineShard shard : entries.values()) {
                shard.release();
                totalShards++;
            }
            entries.clear();
        }
        LOGGER.info("cleanup all slices of {} shards", totalShards);
    }

}
