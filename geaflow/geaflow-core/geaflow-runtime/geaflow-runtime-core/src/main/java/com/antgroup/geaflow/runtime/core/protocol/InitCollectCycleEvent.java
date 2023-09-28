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

import com.antgroup.geaflow.cluster.collector.CollectResponseCollector;
import com.antgroup.geaflow.cluster.collector.InitEmitterRequest;
import com.antgroup.geaflow.collector.ICollector;
import com.antgroup.geaflow.core.graph.ExecutionTask;
import com.antgroup.geaflow.ha.runtime.HighAvailableLevel;
import com.antgroup.geaflow.io.ResponseOutputDesc;
import com.antgroup.geaflow.shuffle.OutputDescriptor;
import com.google.common.base.Preconditions;
import java.util.Collections;
import java.util.List;

/**
 * An assign event provides some runtime execution information for worker to build the cycle pipeline.
 * including: execution task descriptors, shuffle descriptors
 */
public class InitCollectCycleEvent extends InitCycleEvent {

    private static final int COLLECT_BUCKET_NUM = 1;

    public InitCollectCycleEvent(int workerId, int cycleId, long iterationId,
                                 long pipelineId, String pipelineName,
                                 ExecutionTask task, HighAvailableLevel haLevel, long nestedWindowId) {
        super(workerId, cycleId, iterationId, pipelineId, pipelineName, task, haLevel, nestedWindowId);
    }

    @Override
    protected List<ICollector<?>> buildCollectors(OutputDescriptor outputDescriptor, InitEmitterRequest request) {
        Preconditions.checkArgument(outputDescriptor.getOutputDescList().size() == COLLECT_BUCKET_NUM,
            "only support one collect output info yet");
        ResponseOutputDesc outputDesc = (ResponseOutputDesc) outputDescriptor.getOutputDescList().get(0);
        return Collections.singletonList(new CollectResponseCollector<>(outputDesc));
    }

}
