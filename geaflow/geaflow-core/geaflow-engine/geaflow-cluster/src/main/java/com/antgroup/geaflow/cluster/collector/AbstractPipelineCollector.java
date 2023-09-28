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
import com.antgroup.geaflow.common.exception.GeaflowRuntimeException;
import com.antgroup.geaflow.partitioner.IPartitioner;
import com.antgroup.geaflow.partitioner.impl.KeyPartitioner;
import com.antgroup.geaflow.selector.ISelector;
import com.antgroup.geaflow.selector.impl.ChannelSelector;
import com.antgroup.geaflow.shuffle.ForwardOutputDesc;
import java.util.List;
import java.util.stream.IntStream;

public abstract class AbstractPipelineCollector<T> extends AbstractCollector implements
    ICollector<T> {

    protected transient IOutputMessageBuffer<T, ?> outputBuffer;
    protected transient ISelector recordISelector;
    protected ForwardOutputDesc outputDesc;
    protected long windowId;

    public AbstractPipelineCollector(ForwardOutputDesc outputDesc) {
        super(outputDesc.getPartitioner().getOpId());
        this.outputDesc = outputDesc;
    }

    @Override
    public void setUp(RuntimeContext runtimeContext) {
        super.setUp(runtimeContext);
        List<Integer> targetTaskIds = outputDesc.getTargetTaskIndices();
        IPartitioner partitioner = outputDesc.getPartitioner();
        if (partitioner.getPartitionType() == IPartitioner.PartitionType.key) {
            ((KeyPartitioner) partitioner).init(outputDesc.getNumPartitions());
        }
        this.recordISelector = new ChannelSelector(targetTaskIds.size(),
            partitioner);
    }

    @Override
    public int getId() {
        return id;
    }

    public void setOutputBuffer(IOutputMessageBuffer<T, ?> outputBuffer) {
        this.outputBuffer = outputBuffer;
    }

    public long getWindowId() {
        return windowId;
    }

    public void setWindowId(long windowId) {
        this.windowId = windowId;
    }

    @Override
    public void broadcast(T value) {
        List<Integer> targetTaskIds = outputDesc.getTargetTaskIndices();
        int[] channels = IntStream.rangeClosed(0, targetTaskIds.size() - 1).toArray();
        try {
            this.outputBuffer.emit(this.windowId, value, false, channels);
            this.outputMeter.mark();
        } catch (Exception e) {
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
            this.outputBuffer.finish(this.windowId);
        } catch (Exception e) {
            throw new GeaflowRuntimeException(e);
        }
    }

    /**
     * Shuffle data with value itself.
     */
    protected void shuffle(T value, boolean isRetract) {
        int[] targetChannels = this.recordISelector.selectChannels(value);

        try {
            this.outputBuffer.emit(this.windowId, value, isRetract, targetChannels);
            this.outputMeter.mark();
        } catch (Exception e) {
            throw new GeaflowRuntimeException(e);
        }
    }

    /**
     * Shuffle data with key.
     */
    protected <KEY> void shuffle(KEY key, T value, boolean isRetract) {
        int[] targetChannels = this.recordISelector.selectChannels(key);

        try {
            this.outputBuffer.emit(this.windowId, value, isRetract, targetChannels);
            this.outputMeter.mark();
        } catch (Exception e) {
            throw new GeaflowRuntimeException(e);
        }
    }

}
