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

import com.antgroup.geaflow.cluster.fetcher.InitFetchRequest;
import com.antgroup.geaflow.core.graph.ExecutionTask;
import com.antgroup.geaflow.runtime.core.worker.AbstractAlignedWorker;
import com.antgroup.geaflow.runtime.core.worker.context.AbstractWorkerContext;
import com.antgroup.geaflow.runtime.core.worker.fetch.FetchListenerImpl;
import com.antgroup.geaflow.runtime.shuffle.InputDescriptor;
import com.antgroup.geaflow.runtime.shuffle.IoDescriptor;

public abstract class AbstractInitCommand extends AbstractExecutableCommand {

    public AbstractInitCommand(int workerId, int cycleId, long windowId) {
        super(workerId, cycleId, windowId);
    }

    private InitFetchRequest buildInitFetchRequest(long pipelineId,
                                                     String pipelineName,
                                                     ExecutionTask executionTask,
                                                     IoDescriptor ioDescriptor) {
        InputDescriptor inputDescriptor = ioDescriptor.getInputDescriptor();
        return new InitFetchRequest.InitRequestBuilder(pipelineId, pipelineName)
            .setInputShardMap(inputDescriptor.getInputShardMap())
            .setInputStreamMap(inputDescriptor.getStreamId2NameMap())
            .setEncoders(inputDescriptor.getEdgeId2EncoderMap())
            .setDescriptor(inputDescriptor.getShuffleDescriptor())
            .setTaskId(executionTask.getTaskId())
            .setTaskIndex(executionTask.getIndex())
            .setTaskName(executionTask.getTaskName())
            .setVertexId(executionTask.getVertexId());
    }

    /**
     * Init input fetcher.
     */
    protected void initFetchRequest(IoDescriptor ioDescriptor, long pipelineId, String pipelineName) {
        AbstractWorkerContext workerContext = (AbstractWorkerContext) context;
        InitFetchRequest request = buildInitFetchRequest(pipelineId,
            pipelineName, workerContext.getExecutionTask(), ioDescriptor);
        request.addListener(new FetchListenerImpl(((AbstractAlignedWorker) worker).getInputReader()));
        fetcherRunner.add(request);
    }
}
