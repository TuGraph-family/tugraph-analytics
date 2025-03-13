/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
import com.antgroup.geaflow.common.exception.GeaflowRuntimeException;
import com.antgroup.geaflow.common.metric.EventMetrics;
import com.antgroup.geaflow.core.graph.ExecutionTask;
import com.antgroup.geaflow.runtime.core.worker.AbstractWorker;
import com.antgroup.geaflow.runtime.core.worker.InputReader;
import com.antgroup.geaflow.runtime.core.worker.OutputWriter;
import com.antgroup.geaflow.runtime.core.worker.context.WorkerContext;
import com.antgroup.geaflow.shuffle.ForwardOutputDesc;
import com.antgroup.geaflow.shuffle.InputDescriptor;
import com.antgroup.geaflow.shuffle.IoDescriptor;
import com.antgroup.geaflow.shuffle.OutputDescriptor;
import com.antgroup.geaflow.shuffle.desc.IInputDesc;
import com.antgroup.geaflow.shuffle.desc.IOutputDesc;
import com.antgroup.geaflow.shuffle.desc.InputType;
import com.antgroup.geaflow.shuffle.desc.OutputType;
import com.antgroup.geaflow.shuffle.desc.ShardInputDesc;
import com.antgroup.geaflow.shuffle.message.Shard;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class AbstractInitCommand extends AbstractExecutableCommand {

    protected final long pipelineId;
    protected final String pipelineName;
    protected final IoDescriptor ioDescriptor;

    public AbstractInitCommand(long schedulerId,
                               int workerId,
                               int cycleId,
                               long windowId,
                               long pipelineId,
                               String pipelineName,
                               IoDescriptor ioDescriptor) {
        super(schedulerId, workerId, cycleId, windowId);
        this.pipelineId = pipelineId;
        this.pipelineName = pipelineName;
        this.ioDescriptor = ioDescriptor;
    }

    protected void initFetcher() {
        InputDescriptor inputDescriptor = this.ioDescriptor.getInputDescriptor();
        if (inputDescriptor == null || inputDescriptor.getInputDescMap().isEmpty()) {
            return;
        }
        WorkerContext workerContext = (WorkerContext) this.context;
        InitFetchRequest request = this.buildInitFetchRequest(
            inputDescriptor, workerContext.getExecutionTask(), workerContext.getEventMetrics());
        this.fetcherRunner.add(request);
    }

    protected InitFetchRequest buildInitFetchRequest(InputDescriptor inputDescriptor,
                                                     ExecutionTask task,
                                                     EventMetrics eventMetrics) {
        Map<Integer, ShardInputDesc> inputDescMap = new HashMap<>();
        for (Map.Entry<Integer, IInputDesc<?>> entry : inputDescriptor.getInputDescMap().entrySet()) {
            IInputDesc<?> inputDesc = entry.getValue();
            if (inputDesc.getInputType() == InputType.META) {
                inputDescMap.put(entry.getKey(), (ShardInputDesc) entry.getValue());
            }
        }

        InitFetchRequest initFetchRequest = new InitFetchRequest(
            this.pipelineId,
            this.pipelineName,
            task.getVertexId(),
            task.getTaskId(),
            task.getIndex(),
            task.getParallelism(),
            task.getTaskName(),
            inputDescMap);
        InputReader<?> inputReader = ((AbstractWorker<?, ?>) this.worker).getInputReader();
        inputReader.setEventMetrics(eventMetrics);
        initFetchRequest.addListener(inputReader);
        return initFetchRequest;
    }

    protected void initEmitter() {
        OutputDescriptor outputDescriptor = this.ioDescriptor.getOutputDescriptor();
        if (outputDescriptor == null || outputDescriptor.getOutputDescList().isEmpty()) {
            ((WorkerContext) this.context).setCollectors(Collections.emptyList());
            return;
        }
        InitEmitterRequest request = this.buildInitEmitterRequest(outputDescriptor);
        this.emitterRunner.add(request);
        List<ICollector<?>> collectors = this.buildCollectors(outputDescriptor, request);
        ((WorkerContext) this.context).setCollectors(collectors);
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

    protected List<ICollector<?>> buildCollectors(OutputDescriptor outputDescriptor, InitEmitterRequest request) {
        List<IOutputDesc> outputDescList = outputDescriptor.getOutputDescList();
        int outputNum = outputDescList.size();
        List<ICollector<?>> collectors = new ArrayList<>(outputNum);
        List<IOutputMessageBuffer<?, Shard>> outputBuffers = request.getOutputBuffers();
        for (int i = 0; i < outputNum; i++) {
            IOutputDesc outputDesc = outputDescList.get(i);
            IOutputMessageBuffer<?, Shard> outputBuffer = outputBuffers.get(i);
            ICollector<?> collector = CollectorFactory.create(outputDesc);
            if (outputDesc.getType() != OutputType.RESPONSE) {
                ((AbstractPipelineCollector) collector).setOutputBuffer(outputBuffer);
            }
            collectors.add(collector);
        }
        return collectors;
    }

    protected void popEmitter() {
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
            if (outputDesc.getType() != OutputType.RESPONSE) {
                int bucketNum = ((ForwardOutputDesc) outputDesc).getTargetTaskIndices().size();
                outputBuffer = new OutputWriter<>(outputDesc.getEdgeId(), bucketNum);
                outputBuffer.setEventMetrics(((WorkerContext) this.context).getEventMetrics());
            }
            outputBuffers.add(outputBuffer);
        }
        return outputBuffers;
    }

    public long getPipelineId() {
        return this.pipelineId;
    }
}
