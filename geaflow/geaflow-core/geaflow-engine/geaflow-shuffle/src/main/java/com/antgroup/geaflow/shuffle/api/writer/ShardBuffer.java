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

import com.antgroup.geaflow.common.encoder.IEncoder;
import com.antgroup.geaflow.common.metric.ShuffleWriteMetrics;
import com.antgroup.geaflow.shuffle.api.pipeline.buffer.HeapBuffer.HeapBufferBuilder;
import com.antgroup.geaflow.shuffle.api.pipeline.buffer.OutBuffer;
import com.antgroup.geaflow.shuffle.api.pipeline.buffer.OutBuffer.BufferBuilder;
import com.antgroup.geaflow.shuffle.api.pipeline.buffer.PipeBuffer;
import com.antgroup.geaflow.shuffle.api.pipeline.buffer.PipelineSlice;
import com.antgroup.geaflow.shuffle.config.ShuffleConfig;
import com.antgroup.geaflow.shuffle.memory.ShuffleMemoryTracker;
import com.antgroup.geaflow.shuffle.message.PipelineBarrier;
import com.antgroup.geaflow.shuffle.serialize.EncoderRecordSerializer;
import com.antgroup.geaflow.shuffle.serialize.IRecordSerializer;
import com.antgroup.geaflow.shuffle.serialize.RecordSerializer;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public abstract class ShardBuffer<T, R> {

    protected ShuffleConfig shuffleConfig;

    protected long pipelineId;
    protected int edgeId;
    protected int taskIndex;
    protected String pipelineName;

    protected int targetChannels;
    protected String taskLogTag;
    protected List<BufferBuilder> buffers;
    protected PipelineSlice[] resultSlices;
    protected int[] batchCounter;
    protected long[] bytesCounter;
    protected ShuffleMemoryTracker memoryTracker;
    protected ShuffleWriteMetrics writeMetrics;
    protected long maxBufferSize;
    protected IRecordSerializer<T> recordSerializer;

    public void init(IWriterContext writerContext) {
        this.shuffleConfig = ShuffleConfig.getInstance();
        this.memoryTracker = ShuffleMemoryTracker.getInstance();
        this.writeMetrics = new ShuffleWriteMetrics();

        this.targetChannels = writerContext.getTargetChannelNum();
        this.pipelineId = writerContext.getPipelineInfo().getPipelineId();
        this.pipelineName = writerContext.getPipelineInfo().getPipelineName();
        this.edgeId = writerContext.getEdgeId();
        this.taskIndex = writerContext.getTaskIndex();
        this.taskLogTag = writerContext.getTaskName();
        this.recordSerializer = getRecordSerializer(writerContext);

        this.batchCounter = new int[targetChannels];
        this.bytesCounter = new long[targetChannels];
        this.maxBufferSize = this.shuffleConfig.getWriteBufferSizeBytes();
        buildBufferBuilder(targetChannels);
    }

    private void buildBufferBuilder(int channels) {
        this.buffers = new ArrayList<>(channels);
        for (int i = 0; i < channels; i++) {
            BufferBuilder bufferBuilder = new HeapBufferBuilder();
            bufferBuilder.enableMemoryTrack();
            buffers.add(bufferBuilder);
        }
    }

    public void emit(long batchId, T value, boolean isRetract, int[] channels)
        throws IOException {
        for (int channel : channels) {
            BufferBuilder outBuffer = buffers.get(channel);
            recordSerializer.serialize(value, isRetract, outBuffer);
            batchCounter[channel]++;

            if (outBuffer.getBufferSize() >= maxBufferSize) {
                send(channel, outBuffer.build(), batchId);
            }
        }
    }

    public void emit(long batchId, List<T> data, int channel) {
        BufferBuilder outBuffer = this.buffers.get(channel);
        int size = data.size();
        for (int i = 0; i < size; i++) {
            this.recordSerializer.serialize(data.get(i), false, outBuffer);
            this.batchCounter[channel]++;
        }
        if (outBuffer.getBufferSize() >= maxBufferSize) {
            send(channel, outBuffer.build(), batchId);
        }
    }

    protected void send(int selectChannel, OutBuffer outBuffer, long batchId) {
        sendBuffer(selectChannel, outBuffer, batchId);
        this.bytesCounter[selectChannel] += outBuffer.getBufferSize();
    }

    protected void sendBuffer(int sliceIndex, OutBuffer buffer, long batchId) {
        PipelineSlice resultSlice = resultSlices[sliceIndex];
        resultSlice.add(new PipeBuffer(buffer, batchId, true));
    }

    public abstract Optional<R> finish(long batchId) throws IOException;

    public ShuffleWriteMetrics getShuffleWriteMetrics() {
        return this.writeMetrics;
    }

    public void close() {
    }

    protected void notify(PipelineBarrier barrier) throws IOException {
        for (int channel = 0; channel < this.targetChannels; channel++) {
            notify(barrier, channel);
        }
    }

    protected void notify(PipelineBarrier barrier, int channel) {
        long batchId = barrier.getWindowId();
        int recordCount = this.batchCounter[channel];
        sendBarrier(channel, batchId, recordCount, barrier.isFinish());

        this.writeMetrics.increaseRecords(recordCount);
        this.writeMetrics.increaseEncodedSize(this.bytesCounter[channel]);
        this.batchCounter[channel] = 0;
        this.bytesCounter[channel] = 0;
    }

    protected void sendBarrier(int sliceIndex, long batchId, int count, boolean isFinish) {
        PipelineSlice resultSlice = resultSlices[sliceIndex];
        resultSlice.add(new PipeBuffer(batchId, count, false, isFinish));
    }

    @SuppressWarnings("unchecked")
    private static <T> IRecordSerializer<T> getRecordSerializer(IWriterContext writerContext) {
        IEncoder<?> encoder = writerContext.getEncoder();
        if (encoder == null) {
            return new RecordSerializer<>();
        }
        return new EncoderRecordSerializer<>((IEncoder<T>) encoder);
    }

}
