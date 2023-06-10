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

import static com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys.SHUFFLE_CACHE_SPILL_THRESHOLD;

import com.antgroup.geaflow.common.metric.ShuffleWriteMetrics;
import com.antgroup.geaflow.shuffle.api.pipeline.buffer.OutBuffer.BufferBuilder;
import com.antgroup.geaflow.shuffle.api.pipeline.buffer.PipelineShard;
import com.antgroup.geaflow.shuffle.api.pipeline.buffer.PipelineSlice;
import com.antgroup.geaflow.shuffle.config.ShuffleConfig;
import com.antgroup.geaflow.shuffle.memory.ShuffleDataManager;
import com.antgroup.geaflow.shuffle.message.ISliceMeta;
import com.antgroup.geaflow.shuffle.message.PipelineBarrier;
import com.antgroup.geaflow.shuffle.message.PipelineSliceMeta;
import com.antgroup.geaflow.shuffle.message.Shard;
import com.antgroup.geaflow.shuffle.message.ShuffleId;
import com.antgroup.geaflow.shuffle.message.SliceId;
import com.antgroup.geaflow.shuffle.message.WriterId;
import com.antgroup.geaflow.shuffle.network.IConnectionManager;
import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SpillableShardBuffer<T> extends ShardBuffer<T, Shard> {

    private static final Logger LOGGER = LoggerFactory.getLogger(SpillableShardBuffer.class);

    protected boolean cacheEnabled;
    protected double cacheSpillThreshold;
    protected WriterId writerId;
    protected ShuffleId shuffleId;

    protected ShuffleWriteMetrics writeMetrics;
    protected ShuffleConfig shuffleConfig;
    protected IWriterContext writerContext;
    protected IConnectionManager connectionManager;
    protected int taskId;

    public SpillableShardBuffer() {
    }

    public SpillableShardBuffer(IConnectionManager connectionManager) {
        this.connectionManager = connectionManager;
    }

    public void setConnectionManager(IConnectionManager connectionManager) {
        this.connectionManager = connectionManager;
    }

    @Override
    public void init(IWriterContext writerContext) {
        super.init(writerContext);

        this.writerContext = writerContext;
        this.taskId = writerContext.getTaskId();
        this.shuffleConfig = ShuffleConfig.getInstance(config);

        this.writeMetrics = new ShuffleWriteMetrics();

        this.cacheEnabled = writerContext.getShuffleDescriptor().isCacheEnabled();
        if (cacheEnabled) {
            LOGGER.info("cache is enabled in {}", taskLogTag);
        }

        int channels = writerContext.getTargetChannelNum();

        this.writerId = new WriterId(writerContext.getPipelineInfo().getPipelineId(), edgeId, taskIndex);
        int refCount = cacheEnabled ? Integer.MAX_VALUE : 1;
        initResultSlices(channels, refCount);

        this.cacheSpillThreshold = config.getDouble(SHUFFLE_CACHE_SPILL_THRESHOLD);
    }

    private void initResultSlices(int channels, int refCount) {
        ShuffleDataManager shuffleDataManager = ShuffleDataManager.getInstance();
        PipelineShard pipeShard = shuffleDataManager.getShard(writerId);
        if (pipeShard == null) {
            PipelineSlice[] slices = new PipelineSlice[channels];
            for (int i = 0; i < channels; i++) {
                slices[i] = new PipelineSlice(taskLogTag, new SliceId(writerId, i), refCount);
            }
            pipeShard = new PipelineShard(taskLogTag, slices);
        }
        resultSlices = pipeShard.getSlices();
    }

    @Override
    public Optional<Shard> finish(long batchId) throws IOException {
        final long beginTime = System.currentTimeMillis();
        flushFloatingBuffers(batchId);
        List<ISliceMeta> slices = buildSliceMeta(batchId);
        long maxSliceSize = 0;
        for (int i = 0; i < slices.size(); i++) {
            ISliceMeta sliceMeta = slices.get(i);
            if (sliceMeta.getRecordNum() > 0) {
                writeMetrics.increaseWrittenChannels();
                writeMetrics.increaseRecords(sliceMeta.getRecordNum());
                writeMetrics.increaseEncodedSize(sliceMeta.getEncodedSize());
                if (sliceMeta.getEncodedSize() > maxSliceSize) {
                    maxSliceSize = sliceMeta.getEncodedSize();
                }
            }
            buffers.get(i).close();
        }

        writeMetrics.setMaxSliceKB(maxSliceSize / 1024);
        writeMetrics.setNumChannels(slices.size());
        long flushTime = System.currentTimeMillis() - beginTime;
        writeMetrics.setFlushMs(flushTime);
        LOGGER.info("taskId {} {} flush batchId:{} useTime:{}ms {}", taskId, taskLogTag, batchId,
            flushTime, writeMetrics);

        buffers.clear();
        buffers = null;
        batchCounter = null;
        resultSlices = null;
        bytesCounter = null;

        return Optional.of(new Shard(edgeId, slices));
    }

    private List<ISliceMeta> buildSliceMeta(long batchId) {
        List<ISliceMeta> slices = new ArrayList<>();
        PipelineBarrier barrier = new PipelineBarrier(batchId, edgeId, taskIndex);
        barrier.setFinish(true);
        int writtenChannels = 0;
        for (int i = 0; i < targetChannels; i++) {
            SliceId sliceId = resultSlices[i].getSliceId();
            PipelineSliceMeta sliceMeta = new PipelineSliceMeta(sliceId, batchId,
                connectionManager.getShuffleAddress());
            sliceMeta.setRecordNum(batchCounter[i]);
            sliceMeta.setEncodedSize(bytesCounter[i]);
            slices.add(sliceMeta);
            if (sliceMeta.getRecordNum() > 0) {
                notify(barrier, i);
                writtenChannels++;
            }
        }

        if (writtenChannels > 0) {
            ShuffleDataManager.getInstance().register(writerId,
                new PipelineShard(taskLogTag, resultSlices, writtenChannels));
        }
        return slices;
    }

    @VisibleForTesting
    public ShuffleWriteMetrics getWriteMetrics() {
        return writeMetrics;
    }

    @Override
    public void close() {
    }

    protected void flushFloatingBuffers(long batchId) {
        for (int i = 0; i < buffers.size(); i++) {
            BufferBuilder bufferBuilder = buffers.get(i);
            if (bufferBuilder.getBufferSize() > 0) {
                send(i, bufferBuilder.build(), batchId);
            }
        }
    }

}
