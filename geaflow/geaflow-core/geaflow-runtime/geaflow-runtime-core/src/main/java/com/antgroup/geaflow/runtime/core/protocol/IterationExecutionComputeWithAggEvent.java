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
import com.antgroup.geaflow.model.record.RecordArgs;
import com.antgroup.geaflow.runtime.core.worker.AbstractAlignedWorker;
import com.antgroup.geaflow.runtime.io.IInputDesc;
import com.antgroup.geaflow.runtime.shuffle.InputDescriptor;
import com.antgroup.geaflow.shuffle.message.PipelineMessage;
import com.antgroup.geaflow.shuffle.serialize.IMessageIterator;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Send from scheduler to cycle head task to launch one iteration with aggregation of the cycle.
 */
public class IterationExecutionComputeWithAggEvent extends AbstractIterationComputeCommand {

    private final InputDescriptor inputDescriptor;

    public IterationExecutionComputeWithAggEvent(int workerId, int cycleId, long windowId,
                                                 long fetchWindowId, long fetchCount,
                                                 InputDescriptor inputDescriptor) {
        super(workerId, cycleId, windowId, fetchWindowId, fetchCount);
        this.inputDescriptor = inputDescriptor;
    }

    @Override
    public void execute(ITaskContext taskContext) {
        ((AbstractAlignedWorker) taskContext.getWorker()).getInputReader().onMessage(fetchAggResult());
        super.execute(taskContext);
    }

    @Override
    public int getWorkerId() {
        return workerId;
    }

    @Override
    public EventType getEventType() {
        return EventType.ITERATIVE_COMPUTE_WITH_AGGREGATE;
    }

    private PipelineMessage<?> fetchAggResult() {
        List<?> aggRecords = new ArrayList<>();
        for (IInputDesc inputDesc : inputDescriptor.getInputDescMap().values()) {
            aggRecords.addAll(inputDesc.getInput());
        }
        return new PipelineMessage<>(this.fetchWindowId,
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
            + "workerId=" + workerId
            + ", cycleId=" + cycleId
            + ", windowId=" + windowId
            + ", fetchWindowId=" + fetchWindowId
            + ", fetchCount=" + fetchCount
            + '}';
    }
}
