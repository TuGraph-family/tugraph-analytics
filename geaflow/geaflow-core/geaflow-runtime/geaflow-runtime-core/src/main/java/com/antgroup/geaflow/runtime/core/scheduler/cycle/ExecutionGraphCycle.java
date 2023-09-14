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

package com.antgroup.geaflow.runtime.core.scheduler.cycle;

import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.exception.GeaflowRuntimeException;
import com.antgroup.geaflow.ha.runtime.HighAvailableLevel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ExecutionGraphCycle extends AbstractExecutionCycle {

    private final String driverId;
    private final Map<Integer, IExecutionCycle> cycleMap;
    private final Map<Integer, List<Integer>> cycleChildren;
    private final Map<Integer, List<Integer>> cycleParents;
    private HighAvailableLevel haLevel;

    public ExecutionGraphCycle(long pipelineId, String pipelineName, int cycleId,
                               int flyingCount, long iterationCount,
                               Configuration config, String driverId) {
        super(pipelineId, pipelineName, cycleId, flyingCount, iterationCount, config);
        this.driverId = driverId;
        this.cycleMap = new HashMap<>();
        this.cycleChildren = new HashMap<>();
        this.cycleParents = new HashMap<>();
        this.haLevel = HighAvailableLevel.REDO;
    }

    public int getCycleId() {
        return cycleId;
    }

    public int getFlyingCount() {
        return flyingCount;
    }

    public long getIterationCount() {
        return iterationCount;
    }

    @Override
    public ExecutionCycleType getType() {
        return ExecutionCycleType.GRAPH;
    }

    @Override
    public String getDriverId() {
        return driverId;
    }

    @Override
    public HighAvailableLevel getHighAvailableLevel() {
        return haLevel;
    }

    public void addCycle(IExecutionCycle cycle) {
        if (cycleMap.containsKey(cycle.getCycleId())) {
            throw new GeaflowRuntimeException(String.format("cycle {} already added", cycle.getCycleId()));
        }
        cycleMap.put(cycle.getCycleId(), cycle);
        cycleParents.put(cycle.getCycleId(), new ArrayList<>());
        cycleChildren.put(cycle.getCycleId(), new ArrayList<>());

        ExecutionNodeCycle nodeCycle = (ExecutionNodeCycle) cycle;
        cycleParents.get(cycle.getCycleId()).addAll(nodeCycle.getVertexGroup().getParentVertexGroupIds());
        cycleChildren.get(cycle.getCycleId()).addAll(nodeCycle.getVertexGroup().getChildrenVertexGroupIds());

        if (iterationCount > 1 && haLevel != HighAvailableLevel.CHECKPOINT
            && (cycle.getType() == ExecutionCycleType.ITERATION || cycle.getType() == ExecutionCycleType.ITERATION_WITH_AGG)) {
            haLevel = HighAvailableLevel.CHECKPOINT;
        }
    }

    public Map<Integer, IExecutionCycle> getCycleMap() {
        return cycleMap;
    }

    public Map<Integer, List<Integer>> getCycleChildren() {
        return cycleChildren;
    }

    public Map<Integer, List<Integer>> getCycleParents() {
        return cycleParents;
    }
}
