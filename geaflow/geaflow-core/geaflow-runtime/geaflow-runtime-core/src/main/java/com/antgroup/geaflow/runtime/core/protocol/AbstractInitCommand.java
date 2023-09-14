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
import com.antgroup.geaflow.common.encoder.IEncoder;
import com.antgroup.geaflow.common.shuffle.ShuffleDescriptor;
import com.antgroup.geaflow.core.graph.ExecutionTask;
import com.antgroup.geaflow.runtime.core.worker.AbstractAlignedWorker;
import com.antgroup.geaflow.runtime.core.worker.context.AbstractWorkerContext;
import com.antgroup.geaflow.runtime.core.worker.fetch.FetchListenerImpl;
import com.antgroup.geaflow.runtime.io.IInputDesc;
import com.antgroup.geaflow.runtime.shuffle.InputDescriptor;
import com.antgroup.geaflow.runtime.shuffle.IoDescriptor;
import com.antgroup.geaflow.runtime.shuffle.ShardInputDesc;
import com.antgroup.geaflow.shuffle.message.Shard;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class AbstractInitCommand extends AbstractExecutableCommand {

    public AbstractInitCommand(int workerId, int cycleId, long windowId) {
        super(workerId, cycleId, windowId);
    }

    private InitFetchRequest buildInitFetchRequest(long pipelineId,
                                                   String pipelineName,
                                                   ExecutionTask executionTask,
                                                   IoDescriptor ioDescriptor) {
        InputDescriptor inputDescriptor = ioDescriptor.getInputDescriptor();
        Map<Integer, List<Shard>> inputShardMap = new HashMap<>();
        Map<Integer, String> streamId2NameMap = new HashMap<>();
        Map<Integer, IEncoder<?>> edgeId2EncoderMap = new HashMap<>();
        ShuffleDescriptor shuffleDescriptor = null;
        for (IInputDesc inputDesc : inputDescriptor.getInputDescMap().values()) {
            if (inputDesc.getInputType() == IInputDesc.InputType.META) {
                inputShardMap.put(inputDesc.getEdgeId(), inputDesc.getInput());
                edgeId2EncoderMap.put(inputDesc.getEdgeId(), ((ShardInputDesc) inputDesc).getEncoder());
                streamId2NameMap.put(inputDesc.getEdgeId(), inputDesc.getName());
                shuffleDescriptor = ((ShardInputDesc) inputDesc).getShuffleDescriptor();
            }
        }
        return new InitFetchRequest.InitRequestBuilder(pipelineId, pipelineName)
            .setInputShardMap(inputShardMap)
            .setInputStreamMap(streamId2NameMap)
            .setEncoders(edgeId2EncoderMap)
            .setDescriptor(shuffleDescriptor)
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
