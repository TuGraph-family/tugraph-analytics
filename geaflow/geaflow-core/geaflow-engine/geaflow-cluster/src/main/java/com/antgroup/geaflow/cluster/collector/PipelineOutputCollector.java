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
import com.antgroup.geaflow.cluster.response.ShardResult;
import com.antgroup.geaflow.collector.IResultCollector;
import com.antgroup.geaflow.common.exception.GeaflowRuntimeException;
import com.antgroup.geaflow.shuffle.OutputInfo;
import com.antgroup.geaflow.shuffle.message.Shard;
import java.io.IOException;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PipelineOutputCollector<T> extends AbstractPipelineCollector<T> implements IResultCollector<ShardResult> {

    private static final Logger LOGGER = LoggerFactory.getLogger(PipelineOutputCollector.class);

    private int edgeId;
    private String edgeName;
    private Shard shard;

    public PipelineOutputCollector(OutputInfo outputInfo) {
        super(outputInfo);
        this.edgeName = outputInfo.getEdgeName();
        this.edgeId = outputInfo.getEdgeId();
    }

    @Override
    public void setUp(RuntimeContext runtimeContext) {
        super.setUp(runtimeContext);
        int taskId = runtimeContext.getTaskArgs().getTaskId();
        int taskIndex = runtimeContext.getTaskArgs().getTaskIndex();
        this.shard = null;
        LOGGER.info("setup PipelineOutputCollector {} taskId {} taskIndex {} edgeName {} edgeId {}",
            this, taskId, taskIndex, this.edgeName, this.edgeId);
    }

    @Override
    public void finish() {
        try {
            Optional result = this.pipeRecordWriter.flush(this.windowId);
            if (result.isPresent()) {
                shard = (Shard) result.get();
            }
        } catch (IOException e) {
            throw new GeaflowRuntimeException(e);
        }
    }

    @Override
    public String getTag() {
        return edgeName;
    }

    /**
     * Collect shard result.
     *
     * @return
     */
    @Override
    public ShardResult collectResult() {
        if (shard == null) {
            return null;
        }
        return new ShardResult(shard.getEdgeId(), shard.getSlices());
    }
}
