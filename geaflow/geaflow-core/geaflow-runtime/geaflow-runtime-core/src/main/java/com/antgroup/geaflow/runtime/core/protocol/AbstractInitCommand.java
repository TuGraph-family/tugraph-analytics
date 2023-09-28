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

import com.antgroup.geaflow.api.context.RuntimeContext;
import com.antgroup.geaflow.cluster.collector.AbstractPipelineCollector;
import com.antgroup.geaflow.cluster.collector.CollectorFactory;
import com.antgroup.geaflow.cluster.collector.IOutputMessageBuffer;
import com.antgroup.geaflow.cluster.collector.InitEmitterRequest;
import com.antgroup.geaflow.cluster.collector.UpdateEmitterRequest;
import com.antgroup.geaflow.cluster.fetcher.InitFetchRequest;
import com.antgroup.geaflow.collector.ICollector;
import com.antgroup.geaflow.common.encoder.IEncoder;
import com.antgroup.geaflow.common.exception.GeaflowRuntimeException;
import com.antgroup.geaflow.common.shuffle.ShuffleDescriptor;
import com.antgroup.geaflow.core.graph.ExecutionTask;
import com.antgroup.geaflow.core.graph.util.ExecutionTaskUtils;
import com.antgroup.geaflow.io.CollectType;
import com.antgroup.geaflow.runtime.core.worker.AbstractAlignedWorker;
import com.antgroup.geaflow.runtime.core.worker.InputReader;
import com.antgroup.geaflow.runtime.core.worker.OutputWriter;
import com.antgroup.geaflow.runtime.core.worker.context.WorkerContext;
import com.antgroup.geaflow.runtime.io.IInputDesc;
import com.antgroup.geaflow.runtime.shuffle.InputDescriptor;
import com.antgroup.geaflow.runtime.shuffle.IoDescriptor;
import com.antgroup.geaflow.runtime.shuffle.ShardInputDesc;
import com.antgroup.geaflow.shuffle.ForwardOutputDesc;
import com.antgroup.geaflow.shuffle.IOutputDesc;
import com.antgroup.geaflow.shuffle.OutputDescriptor;
import com.antgroup.geaflow.shuffle.message.Shard;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class AbstractInitCommand extends AbstractExecutableCommand {

    protected final long pipelineId;
    protected final String pipelineName;
    protected IoDescriptor ioDescriptor;

    public AbstractInitCommand(int workerId, int cycleId, long windowId, long pipelineId, String pipelineName) {
        super(workerId, cycleId, windowId);
        this.pipelineId = pipelineId;
        this.pipelineName = pipelineName;
    }

    public void setIoDescriptor(IoDescriptor ioDescriptor) {
        this.ioDescriptor = ioDescriptor;
    }

    protected void initFetcher() {
        WorkerContext workerContext = (WorkerContext) this.context;
        if (!(ExecutionTaskUtils.isCycleHead(workerContext.getExecutionTask())
            && this.ioDescriptor.getInputTaskNum() == 0)) {
            InitFetchRequest request = this.buildInitFetchRequest(workerContext.getExecutionTask());
            InputReader<?> inputReader = ((AbstractAlignedWorker<?, ?>) this.worker).getInputReader();
            inputReader.setEventMetrics(workerContext.getEventMetrics());
            request.addListener(inputReader);
            this.fetcherRunner.add(request);
        }
    }

    private InitFetchRequest buildInitFetchRequest(ExecutionTask executionTask) {
        InputDescriptor inputDescriptor = this.ioDescriptor.getInputDescriptor();
        Map<Integer, List<Shard>> inputShardMap = new HashMap<>();
        Map<Integer, String> streamId2NameMap = new HashMap<>();
        Map<Integer, IEncoder<?>> edgeId2EncoderMap = new HashMap<>();
        ShuffleDescriptor shuffleDescriptor = null;
        for (IInputDesc<Shard> inputDesc : inputDescriptor.getInputDescMap().values()) {
            if (inputDesc.getInputType() == IInputDesc.InputType.META) {
                inputShardMap.put(inputDesc.getEdgeId(), inputDesc.getInput());
                edgeId2EncoderMap.put(inputDesc.getEdgeId(), ((ShardInputDesc) inputDesc).getEncoder());
                streamId2NameMap.put(inputDesc.getEdgeId(), inputDesc.getName());
                shuffleDescriptor = ((ShardInputDesc) inputDesc).getShuffleDescriptor();
            }
        }
        return new InitFetchRequest.InitRequestBuilder(this.pipelineId, this.pipelineName)
            .setInputShardMap(inputShardMap)
            .setInputStreamMap(streamId2NameMap)
            .setEncoders(edgeId2EncoderMap)
            .setDescriptor(shuffleDescriptor)
            .setTaskId(executionTask.getTaskId())
            .setTaskIndex(executionTask.getIndex())
            .setTaskName(executionTask.getTaskName())
            .setVertexId(executionTask.getVertexId());
    }

    protected void initEmitter() {
        OutputDescriptor outputDescriptor = this.ioDescriptor.getOutputDescriptor();
        if (outputDescriptor == null) {
            return;
        }
        InitEmitterRequest request = this.buildInitEmitterRequest(outputDescriptor);
        this.emitterRunner.add(request);

        List<ICollector<?>> collectors = this.buildCollectors(outputDescriptor, request);
        ((WorkerContext) this.context).setCollectors(collectors);
    }

    protected List<ICollector<?>> buildCollectors(OutputDescriptor outputDescriptor, InitEmitterRequest request) {
        List<IOutputDesc> outputDescList = outputDescriptor.getOutputDescList();
        int outputNum = outputDescList.size();
        List<ICollector<?>> collectors = new ArrayList<>(outputNum);
        List<IOutputMessageBuffer<?, Shard>> outputBuffers = request.getOutputBuffers();
        for (int i = 0; i < outputNum; i++) {
            IOutputDesc outputDesc = outputDescList.get(i);
            IOutputMessageBuffer<?, Shard> outputBuffer = outputBuffers.get(i);
            ICollector<?> collector = CollectorFactory.create(outputDesc);
            if (outputDesc.getType() != CollectType.RESPONSE) {
                ((AbstractPipelineCollector) collector).setOutputBuffer(outputBuffer);
            }
            collectors.add(collector);
        }
        return collectors;
    }

    private InitEmitterRequest buildInitEmitterRequest(OutputDescriptor outputDescriptor) {
        List<IOutputMessageBuffer<?, Shard>> outputBuffers = this.getOutputBuffers(outputDescriptor.getOutputDescList());
        RuntimeContext runtimeContext = ((WorkerContext) this.context).getRuntimeContext();
        return new InitEmitterRequest(
            runtimeContext.getConfiguration(),
            this.windowId,
            runtimeContext.getPipelineId(),
            runtimeContext.getPipelineName(),
            runtimeContext.getTaskArgs(),
            outputDescriptor,
            outputBuffers);
    }

    protected void updateEmitter() {
        OutputDescriptor outputDescriptor = ioDescriptor.getOutputDescriptor();
        if (outputDescriptor == null) {
            return;
        }

        WorkerContext workerContext = (WorkerContext) this.context;
        List<IOutputDesc> outputDescList = outputDescriptor.getOutputDescList();
        int outputNum = outputDescList.size();
        List<ICollector<?>> collectors = workerContext.getCollectors();
        if (collectors.size() != outputNum) {
            throw new GeaflowRuntimeException(String.format("collector num %d not match output desc num %d", collectors.size(), outputNum));
        }

        List<IOutputMessageBuffer<?, Shard>> outputBuffers = this.getOutputBuffers(outputDescList);
        for (int i = 0; i < outputNum; i++) {
            if (collectors.get(i) instanceof AbstractPipelineCollector) {
                AbstractPipelineCollector collector = (AbstractPipelineCollector) collectors.get(i);
                IOutputMessageBuffer<?, Shard> outputBuffer = outputBuffers.get(i);
                collector.setOutputBuffer(outputBuffer);
            }
        }

        UpdateEmitterRequest updateEmitterRequest =
            new UpdateEmitterRequest(workerContext.getTaskId(), this.windowId, this.pipelineId, this.pipelineName, outputBuffers);
        this.emitterRunner.add(updateEmitterRequest);
    }

    private List<IOutputMessageBuffer<?, Shard>> getOutputBuffers(List<IOutputDesc> outputDescList) {
        int outputNum = outputDescList.size();
        List<IOutputMessageBuffer<?, Shard>> outputBuffers = new ArrayList<>(outputNum);
        for (IOutputDesc outputDesc : outputDescList) {
            OutputWriter<?> outputBuffer = null;
            if (outputDesc.getType() != CollectType.RESPONSE) {
                int bucketNum = ((ForwardOutputDesc) outputDesc).getTargetTaskIndices().size();
                outputBuffer = new OutputWriter<>(outputDesc.getEdgeId(), bucketNum);
                outputBuffer.setEventMetrics(((WorkerContext) this.context).getEventMetrics());
            }
            outputBuffers.add(outputBuffer);
        }
        return outputBuffers;
    }

}
