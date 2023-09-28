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

package com.antgroup.geaflow.runtime.core.scheduler;

import com.antgroup.geaflow.core.graph.ExecutionGraph;
import com.antgroup.geaflow.core.graph.ExecutionTask;
import com.antgroup.geaflow.core.graph.ExecutionVertex;
import com.antgroup.geaflow.core.graph.ExecutionVertexGroup;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExecutionCycleTaskAssigner {

    private static final Logger LOGGER = LoggerFactory.getLogger(ExecutionCycleTaskAssigner.class);

    private static AtomicInteger taskId = new AtomicInteger(0);

    public static Map<Integer, List<ExecutionTask>> assign(ExecutionGraph executionGraph) {

        Map<Integer, List<ExecutionTask>> vertex2Tasks = new HashMap<>();

        for (ExecutionVertexGroup vertexGroup : executionGraph.getVertexGroupMap().values()) {
            for (ExecutionVertex vertex : vertexGroup.getVertexMap().values()) {
                List<ExecutionTask> tasks = new ArrayList<>();
                List<Integer> taskIds = new ArrayList<>();
                for (int i = 0; i < vertex.getParallelism(); i++) {
                    ExecutionTask task = new ExecutionTask(taskId.getAndIncrement(),
                        i, vertex.getParallelism(), vertex.getMaxParallelism(), vertex.getNumPartitions(), vertex.getVertexId());
                    task.setIterative(vertexGroup.getCycleGroupMeta().isIterative());
                    tasks.add(task);
                    taskIds.add(task.getTaskId());
                }
                LOGGER.info("assign task vertexId:{}, taskIds:{}", vertex.getVertexId(), taskIds);

                vertex2Tasks.put(vertex.getVertexId(), tasks);
            }
        }
        return vertex2Tasks;
    }
}
