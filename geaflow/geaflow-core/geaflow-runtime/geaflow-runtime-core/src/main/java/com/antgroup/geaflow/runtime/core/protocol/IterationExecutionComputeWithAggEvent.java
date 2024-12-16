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

package com.antgroup.geaflow.runtime.core.protocol;

import com.antgroup.geaflow.cluster.protocol.EventType;
import com.antgroup.geaflow.cluster.task.ITaskContext;
import com.antgroup.geaflow.common.exception.GeaflowRuntimeException;
import com.antgroup.geaflow.model.record.RecordArgs;
import com.antgroup.geaflow.runtime.core.worker.AbstractAlignedWorker;
import com.antgroup.geaflow.shuffle.IoDescriptor;
import com.antgroup.geaflow.shuffle.desc.IInputDesc;
import com.antgroup.geaflow.shuffle.message.PipelineMessage;
import com.antgroup.geaflow.shuffle.serialize.IMessageIterator;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Send from scheduler to cycle head task to launch one iteration with aggregation of the cycle.
 */
public class IterationExecutionComputeWithAggEvent extends AbstractIterationComputeCommand {

    private final IoDescriptor ioDescriptor;

    public IterationExecutionComputeWithAggEvent(long schedulerId,
                                                 int workerId,
                                                 int cycleId,
                                                 long windowId,
                                                 long fetchWindowId,
                                                 long fetchCount,
                                                 IoDescriptor ioDescriptor) {
        super(schedulerId, workerId, cycleId, windowId, fetchWindowId, fetchCount);
        this.ioDescriptor = ioDescriptor;
    }

    @Override
    public void execute(ITaskContext taskContext) {
        ((AbstractAlignedWorker) taskContext.getWorker()).getInputReader().onMessage(fetchAggResult());
        super.execute(taskContext);
    }

    @Override
    public EventType getEventType() {
        return EventType.ITERATIVE_COMPUTE_WITH_AGGREGATE;
    }

    private PipelineMessage<?> fetchAggResult() {
        List<?> aggRecords = new ArrayList<>();
        List<IInputDesc<?>> inputDesc = new ArrayList<>(this.ioDescriptor.getInputDescriptor().getInputDescMap().values());
        if (inputDesc.size() != 1) {
            throw new GeaflowRuntimeException("agg result should only have 1 input, but found " + inputDesc.size());
        }
        IInputDesc aggDesc = inputDesc.get(0);
        int edgeId = aggDesc.getEdgeId();
        aggRecords.addAll(aggDesc.getInput());
        return new PipelineMessage<>(edgeId, this.fetchWindowId,
            RecordArgs.GraphRecordNames.Aggregate.name(),
            new DataMessageIterator<>(aggRecords));
    }

    private class DataMessageIterator<T> implements IMessageIterator<T> {

        private final Iterator<T> iterator;
        private long size = 0;

        public DataMessageIterator(List<T> data) {
            this.iterator = data.iterator();
            this.size = data.size();
        }

        @Override
        public long getSize() {
            return size;
        }

        @Override
        public void close() {
        }

        @Override
        public boolean hasNext() {
            return iterator.hasNext();
        }

        @Override
        public T next() {
            return iterator.next();
        }
    }

    @Override
    public String toString() {
        return "IterationExecutionComputeWithAggEvent{"
            + "schedulerId=" + schedulerId
            + ", workerId=" + workerId
            + ", cycleId=" + cycleId
            + ", windowId=" + windowId
            + ", fetchWindowId=" + fetchWindowId
            + ", fetchCount=" + fetchCount
            + '}';
    }
}
