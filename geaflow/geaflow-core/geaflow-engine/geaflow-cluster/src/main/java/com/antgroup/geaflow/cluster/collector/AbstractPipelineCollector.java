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

package com.antgroup.geaflow.cluster.collector;

import com.antgroup.geaflow.api.context.RuntimeContext;
import com.antgroup.geaflow.collector.AbstractCollector;
import com.antgroup.geaflow.collector.ICollector;
import com.antgroup.geaflow.common.encoder.IEncoder;
import com.antgroup.geaflow.common.exception.GeaflowRuntimeException;
import com.antgroup.geaflow.partitioner.IPartitioner;
import com.antgroup.geaflow.partitioner.impl.KeyPartitioner;
import com.antgroup.geaflow.selector.ISelector;
import com.antgroup.geaflow.selector.impl.ChannelSelector;
import com.antgroup.geaflow.shuffle.OutputInfo;
import com.antgroup.geaflow.shuffle.api.writer.IShuffleWriter;
import com.antgroup.geaflow.shuffle.api.writer.IWriterContext;
import com.antgroup.geaflow.shuffle.api.writer.WriterContext;
import com.antgroup.geaflow.shuffle.service.ShuffleManager;
import java.io.IOException;
import java.util.List;
import java.util.stream.IntStream;

public abstract class AbstractPipelineCollector<T> extends AbstractCollector implements
    ICollector<T> {

    protected transient IShuffleWriter pipeRecordWriter;
    protected transient ISelector recordISelector;
    protected OutputInfo outputInfo;
    protected long windowId;

    public AbstractPipelineCollector(OutputInfo outputInfo) {
        super(outputInfo.getPartitioner().getOpId());
        this.outputInfo = outputInfo;
    }

    @Override
    public void setUp(RuntimeContext runtimeContext) {
        super.setUp(runtimeContext);
        List<Integer> targetTaskIds = outputInfo.getTargetTaskIndices();
        IPartitioner partitioner = outputInfo.getPartitioner();
        if (partitioner.getPartitionType() == IPartitioner.PartitionType.key) {
            ((KeyPartitioner) partitioner).init(outputInfo.getNumPartitions());
        }
        this.recordISelector = new ChannelSelector(targetTaskIds.size(),
            partitioner);

        this.pipeRecordWriter = ShuffleManager.getInstance().loadShuffleWriter();

        IEncoder<?> encoder = this.outputInfo.getEncoder();
        if (encoder != null) {
            encoder.init(runtimeContext.getConfiguration());
        }
        IWriterContext writerContext = WriterContext.newBuilder()
            .setPipelineId(runtimeContext.getPipelineId())
            .setPipelineName(runtimeContext.getPipelineName())
            .setVertexId(id)
            .setEdgeId(outputInfo.getEdgeId())
            .setTaskId(runtimeContext.getTaskArgs().getTaskId())
            .setTaskIndex(runtimeContext.getTaskArgs().getTaskIndex())
            .setTaskName(runtimeContext.getTaskArgs().getTaskName())
            .setChannelNum(targetTaskIds.size())
            .setConfig(runtimeContext.getConfiguration())
            .setShuffleDescriptor(outputInfo.getShuffleDescriptor())
            .setEncoder(encoder);
        this.pipeRecordWriter.init(writerContext);
    }

    @Override
    public int getId() {
        return id;
    }

    public long getWindowId() {
        return windowId;
    }

    public void setWindowId(long windowId) {
        this.windowId = windowId;
    }

    @Override
    public void broadcast(T value) {
        List<Integer> targetTaskIds = outputInfo.getTargetTaskIndices();
        int[] channels = IntStream.rangeClosed(0, targetTaskIds.size() - 1).toArray();
        try {
            pipeRecordWriter.emit(windowId, value, false, channels);
            this.outputMeter.mark();
        } catch (IOException e) {
            throw new GeaflowRuntimeException(e);
        }
    }

    @Override
    public void partition(T value) {
        shuffle(value, false);
    }

    @Override
    public <KEY> void partition(KEY key, T value) {
        shuffle(key, value, false);
    }

    @Override
    public void finish() {
        try {
            pipeRecordWriter.flush(windowId);
        } catch (IOException e) {
            throw new GeaflowRuntimeException(e);
        }
    }

    @Override
    public void close() {
        if (pipeRecordWriter != null) {
            pipeRecordWriter.close();
        }
    }

    /**
     * Shuffle data with value itself.
     */
    protected void shuffle(T value, boolean isRetract) {
        int[] targetChannels = this.recordISelector.selectChannels(value);

        try {
            pipeRecordWriter.emit(windowId, value, isRetract, targetChannels);
            this.outputMeter.mark();
        } catch (IOException e) {
            throw new GeaflowRuntimeException(e);
        }
    }

    /**
     * Shuffle data with key.
     */
    protected <KEY> void shuffle(KEY key, T value, boolean isRetract) {
        int[] targetChannels = this.recordISelector.selectChannels(key);

        try {
            pipeRecordWriter.emit(windowId, value, isRetract, targetChannels);
            this.outputMeter.mark();
        } catch (IOException e) {
            throw new GeaflowRuntimeException(e);
        }
    }

    public void setOutputInfo(OutputInfo outputInfo) {
        this.outputInfo = outputInfo;
    }
}
