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
import com.antgroup.geaflow.core.graph.ExecutionGraph;
import com.antgroup.geaflow.core.graph.ExecutionTask;
import com.antgroup.geaflow.core.graph.ExecutionTaskType;
import com.antgroup.geaflow.core.graph.ExecutionVertex;
import com.antgroup.geaflow.core.graph.ExecutionVertexGroup;
import com.antgroup.geaflow.plan.graph.VertexType;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ExecutionCycleBuilder {

    private static final int GRAPH_CYCLE_ID = 0;

    /**
     * Build cycle by execution graph.
     */
    public static IExecutionCycle buildExecutionCycle(ExecutionGraph executionGraph,
                                                      Map<Integer, List<ExecutionTask>> vertex2Tasks,
                                                      Configuration config,
                                                      long pipelineId,
                                                      long pipelineTaskId,
                                                      String name,
                                                      long schedulerId,
                                                      String driverId,
                                                      int driverIndex) {

        int flyingCount = executionGraph.getCycleGroupMeta().getFlyingCount();
        long iterationCount = executionGraph.getCycleGroupMeta().getIterationCount();
        ExecutionGraphCycle graphCycle = new ExecutionGraphCycle(schedulerId, pipelineId,
            pipelineTaskId, name, GRAPH_CYCLE_ID, flyingCount, iterationCount, config, driverId, driverIndex);
        for (ExecutionVertexGroup vertexGroup : executionGraph.getVertexGroupMap().values()) {
            ExecutionNodeCycle nodeCycle = buildExecutionCycle(vertexGroup,
                vertex2Tasks, config, pipelineId, pipelineTaskId, name, schedulerId, driverId, driverIndex);
            graphCycle.addCycle(nodeCycle);
        }
        return graphCycle;
    }

    private static ExecutionNodeCycle buildExecutionCycle(ExecutionVertexGroup vertexGroup,
                                                          Map<Integer, List<ExecutionTask>> vertex2Tasks,
                                                          Configuration config,
                                                          long pipelineId,
                                                          long pipelineTaskId,
                                                          String name,
                                                          long schedulerId,
                                                          String driverId,
                                                          int driverIndex) {
        ExecutionNodeCycle cycle;
        if (vertexGroup.getVertexMap().size() == 1
            && vertexGroup.getVertexMap().values().iterator().next().getChainTailType() == VertexType.collect) {
            cycle = new CollectExecutionNodeCycle(schedulerId, pipelineId, pipelineTaskId, name,
                vertexGroup, config, driverId, driverIndex);
        } else {
            cycle = new ExecutionNodeCycle(schedulerId, pipelineId, pipelineTaskId, name,
                vertexGroup, config, driverId, driverIndex);
        }
        List<ExecutionTask> allTasks = new ArrayList<>();
        List<ExecutionTask> headTasks = new ArrayList<>();
        List<ExecutionTask> tailTasks = new ArrayList<>();
        List<String> opNames = new ArrayList<>();

        for (ExecutionVertex vertex : vertexGroup.getVertexMap().values()) {
            boolean isHead = false;
            // is head
            if (vertexGroup.getHeadVertexIds().contains(vertex.getVertexId())) {
                isHead = true;
            }
            boolean isTail = false;
            // is tail
            if (vertexGroup.getTailVertexIds().contains(vertex.getVertexId())) {
                isTail = true;
                opNames.add(vertex.getName());
            }

            List<ExecutionTask> tasks = vertex2Tasks.get(vertex.getVertexId());
            allTasks.addAll(tasks);

            for (ExecutionTask task : tasks) {
                if (isHead && isTail) {
                    task.setExecutionTaskType(ExecutionTaskType.singularity);
                    headTasks.add(task);
                    tailTasks.add(task);
                } else if (isHead) {
                    task.setExecutionTaskType(ExecutionTaskType.head);
                    headTasks.add(task);
                } else if (isTail) {
                    task.setExecutionTaskType(ExecutionTaskType.tail);
                    tailTasks.add(task);
                } else {
                    task.setExecutionTaskType(ExecutionTaskType.middle);
                }
                task.setProcessor(vertex.getProcessor());

            }
        }

        cycle.setName(String.join("|", opNames));
        cycle.setTasks(allTasks);
        cycle.setCycleHeads(headTasks);
        cycle.setCycleTails(tailTasks);
        cycle.setVertexIdToTasks(vertex2Tasks);
        return cycle;
    }
}
