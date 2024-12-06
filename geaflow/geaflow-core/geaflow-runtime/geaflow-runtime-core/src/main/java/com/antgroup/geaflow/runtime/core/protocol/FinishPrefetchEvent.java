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

import com.antgroup.geaflow.cluster.fetcher.CloseFetchRequest;
import com.antgroup.geaflow.cluster.protocol.EventType;
import com.antgroup.geaflow.cluster.task.ITaskContext;
import com.antgroup.geaflow.runtime.core.worker.PrefetchCallbackHandler;
import com.antgroup.geaflow.shuffle.message.SliceId;
import java.util.List;

public class FinishPrefetchEvent extends AbstractExecutableCommand {

    private final int taskId;
    private final int taskIndex;
    private final long pipelineId;
    private final List<Integer> edgeIds;

    public FinishPrefetchEvent(long schedulerId,
                               int workerId,
                               int cycleId,
                               long windowId,
                               int taskId,
                               int taskIndex,
                               long pipelineId,
                               List<Integer> edgeIds) {
        super(schedulerId, workerId, cycleId, windowId);
        this.taskId = taskId;
        this.taskIndex = taskIndex;
        this.pipelineId = pipelineId;
        this.edgeIds = edgeIds;
    }

    @Override
    public void execute(ITaskContext taskContext) {
        super.execute(taskContext);
        PrefetchCallbackHandler callbackHandler = PrefetchCallbackHandler.getInstance();
        for (Integer edgeId : this.edgeIds) {
            SliceId sliceId = new SliceId(this.pipelineId, edgeId, -1, this.taskIndex);
            PrefetchCallbackHandler.PrefetchCallback callback = callbackHandler.removeTaskEventCallback(sliceId);
            callback.execute();
        }

        this.fetcherRunner.add(new CloseFetchRequest(this.taskId));
    }

    @Override
    public EventType getEventType() {
        return EventType.PREFETCH;
    }

    @Override
    public String toString() {
        return "FinishPrefetchEvent{"
            + "taskId=" + taskId
            + ", workerId=" + workerId
            + ", cycleId=" + cycleId
            + ", windowId=" + windowId
            + '}';
    }

}
