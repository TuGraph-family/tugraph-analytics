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
import com.antgroup.geaflow.core.graph.ExecutionTask;
import com.antgroup.geaflow.core.graph.ExecutionVertexGroup;
import com.antgroup.geaflow.ha.runtime.HighAvailableLevel;
import java.util.List;
import java.util.Map;

public class ExecutionNodeCycle extends AbstractExecutionCycle {

    private String name;
    private ExecutionCycleType type;
    private String driverId;
    private HighAvailableLevel highAvailableLevel;

    private ExecutionVertexGroup vertexGroup;
    private List<ExecutionTask> tasks;
    private List<ExecutionTask> cycleHeads;
    private List<ExecutionTask> cycleTails;
    private Map<Integer, List<ExecutionTask>> vertexIdToTasks;
    private boolean isPipelineDataLoop;
    private boolean isCollectResult;
    private transient boolean workerAssigned;

    public ExecutionNodeCycle(long pipelineId, String pipelineName, ExecutionVertexGroup vertexGroup,
                              Configuration config, String driverId) {
        super(pipelineId, pipelineName, vertexGroup.getGroupId(),
            vertexGroup.getCycleGroupMeta().getFlyingCount(), vertexGroup.getCycleGroupMeta().getIterationCount(),
            config);
        this.vertexGroup = vertexGroup;
        if (vertexGroup.getCycleGroupMeta().isIterative()) {
            if (vertexGroup.getVertexMap().size() > 1) {
                this.type = ExecutionCycleType.ITERATION_WITH_AGG;
            } else {
                this.type = ExecutionCycleType.ITERATION;
            }
        } else {
            this.type = ExecutionCycleType.PIPELINE;
        }
        this.isPipelineDataLoop = vertexGroup.getCycleGroupMeta().isIterative();
        this.driverId = driverId;
        if (!vertexGroup.getCycleGroupMeta().isIterative() && vertexGroup.getCycleGroupMeta().getIterationCount() > 1) {
            this.highAvailableLevel = HighAvailableLevel.CHECKPOINT;
        } else {
            this.highAvailableLevel = HighAvailableLevel.REDO;
        }
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public ExecutionVertexGroup getVertexGroup() {
        return vertexGroup;
    }

    public List<ExecutionTask> getTasks() {
        return tasks;
    }

    public void setTasks(List<ExecutionTask> tasks) {
        this.tasks = tasks;
    }

    public List<ExecutionTask> getCycleHeads() {
        return cycleHeads;
    }

    public void setCycleHeads(List<ExecutionTask> cycleHeads) {
        this.cycleHeads = cycleHeads;
    }

    public List<ExecutionTask> getCycleTails() {
        return cycleTails;
    }

    public void setCycleTails(List<ExecutionTask> cycleTails) {
        this.cycleTails = cycleTails;
    }

    public Map<Integer, List<ExecutionTask>> getVertexIdToTasks() {
        return vertexIdToTasks;
    }

    public void setVertexIdToTasks(Map<Integer, List<ExecutionTask>> vertexIdToTasks) {
        this.vertexIdToTasks = vertexIdToTasks;
    }

    @Override
    public ExecutionCycleType getType() {
        return type;
    }

    @Override
    public String getDriverId() {
        return driverId;
    }

    @Override
    public HighAvailableLevel getHighAvailableLevel() {
        return highAvailableLevel;
    }

    public boolean isPipelineDataLoop() {
        return isPipelineDataLoop;
    }

    public boolean isCollectResult() {
        return isCollectResult;
    }

    public void setCollectResult(boolean collectResult) {
        isCollectResult = collectResult;
    }

    public boolean isWorkerAssigned() {
        return workerAssigned;
    }

    public void setWorkerAssigned(boolean workerAssigned) {
        this.workerAssigned = workerAssigned;
    }

}
