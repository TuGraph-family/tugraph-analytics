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

package com.antgroup.geaflow.runtime.core.scheduler.resource;

import static com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys.JOB_UNIQUE_ID;
import static com.antgroup.geaflow.common.config.keys.FrameworkConfigKeys.SYSTEM_STATE_BACKEND_TYPE;

import com.antgroup.geaflow.cluster.system.ClusterMetaStore;
import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.core.graph.ExecutionTask;
import com.antgroup.geaflow.core.graph.ExecutionVertex;
import com.antgroup.geaflow.core.graph.ExecutionVertexGroup;
import com.antgroup.geaflow.runtime.core.scheduler.cycle.ExecutionNodeCycle;
import com.antgroup.geaflow.state.StoreType;
import java.util.ArrayList;
import java.util.List;

public abstract class BaseScheduledWorkerManagerTest {

    protected ExecutionNodeCycle buildMockCycle(int parallelism) {
        Configuration configuration = new Configuration();
        configuration.put(JOB_UNIQUE_ID, "test-scheduler-context");
        configuration.put(SYSTEM_STATE_BACKEND_TYPE.getKey(), StoreType.MEMORY.name());
        ClusterMetaStore.init(0, configuration);

        long finishIterationId = 100;
        ExecutionVertexGroup vertexGroup = new ExecutionVertexGroup(1);
        vertexGroup.getCycleGroupMeta().setFlyingCount(1);
        vertexGroup.getCycleGroupMeta().setIterationCount(finishIterationId);
        vertexGroup.getCycleGroupMeta().setIterative(false);

        ExecutionVertex vertex = new ExecutionVertex(0, "test");
        int taskNum = parallelism;
        vertex.setParallelism(taskNum);
        vertexGroup.getVertexMap().put(0, vertex);
        List<ExecutionTask> tasks = new ArrayList<>();
        for (int i = 0; i < taskNum; i++) {
            ExecutionTask task = new ExecutionTask(i, i, taskNum, taskNum, taskNum, 0);
            tasks.add(task);
        }
        ExecutionNodeCycle cycle =  new ExecutionNodeCycle(0, "test", vertexGroup, configuration, "driver_id");
        cycle.setTasks(tasks);
        return cycle;
    }

}
