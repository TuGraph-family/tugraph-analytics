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

import com.antgroup.geaflow.cluster.collector.InitCollectEmitterRequest;
import com.antgroup.geaflow.cluster.collector.InitEmitterRequest;
import com.antgroup.geaflow.collector.ICollector;
import com.antgroup.geaflow.common.exception.GeaflowRuntimeException;
import com.antgroup.geaflow.core.graph.ExecutionTask;
import com.antgroup.geaflow.ha.runtime.HighAvailableLevel;
import com.antgroup.geaflow.operator.base.AbstractOperator;
import com.antgroup.geaflow.processor.impl.AbstractProcessor;
import com.antgroup.geaflow.runtime.core.worker.AbstractAlignedWorker;
import com.antgroup.geaflow.shuffle.OutputDescriptor;
import java.util.List;

/**
 * An assign event provides some runtime execution information for worker to build the cycle pipeline.
 * including: execution task descriptors, shuffle descriptors
 */
public class InitCollectCycleEvent extends InitCycleEvent {

    public InitCollectCycleEvent(int workerId, int cycleId, long iterationId,
                                 long pipelineId, String pipelineName,
                                 ExecutionTask task, HighAvailableLevel haLevel, long nestedWindowId) {
        super(workerId, cycleId, iterationId, pipelineId, pipelineName, task, haLevel, nestedWindowId);
    }

    /**
     * Init output emitter.
     */
    protected List<ICollector> initEmitterRequest(OutputDescriptor outputDescriptor) {

        InitEmitterRequest request = new InitCollectEmitterRequest(getCollectOpId((AbstractOperator) ((AbstractProcessor) getTask().getProcessor()).getOperator()));
        emitterRunner.add(request);
        ((AbstractAlignedWorker) worker).getOutputWriter()
            .setCollectors(request.getCollectors());
        return request.getCollectors();
    }

    private Integer getCollectOpId(AbstractOperator operator) {
        if (operator.getNextOperators().isEmpty()) {
            return operator.getOpArgs().getOpId();
        } else if (operator.getNextOperators().size() == 1) {
            return getCollectOpId((AbstractOperator) operator.getNextOperators().get(0));
        } else {
            throw new GeaflowRuntimeException("not support collect multi-output");
        }
    }
}
