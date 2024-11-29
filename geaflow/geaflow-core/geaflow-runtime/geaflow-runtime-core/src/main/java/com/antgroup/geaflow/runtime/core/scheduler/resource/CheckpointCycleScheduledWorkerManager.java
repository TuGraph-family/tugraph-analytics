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

import com.antgroup.geaflow.cluster.resourcemanager.WorkerInfo;
import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.exception.GeaflowRuntimeException;
import com.antgroup.geaflow.core.graph.ExecutionTask;
import com.antgroup.geaflow.operator.Operator;
import com.antgroup.geaflow.operator.impl.graph.algo.vc.AbstractGraphVertexCentricOp;
import com.antgroup.geaflow.plan.graph.AffinityLevel;
import com.antgroup.geaflow.processor.impl.AbstractProcessor;
import com.antgroup.geaflow.runtime.core.scheduler.cycle.ExecutionNodeCycle;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CheckpointCycleScheduledWorkerManager extends AbstractScheduledWorkerManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(CheckpointCycleScheduledWorkerManager.class);

    private static volatile CheckpointCycleScheduledWorkerManager INSTANCE = null;

    private CheckpointCycleScheduledWorkerManager(Configuration config) {
        super(config);
    }

    @Override
    public List<WorkerInfo> assign(ExecutionNodeCycle cycle) {

        boolean isWorkerAssigned = cycle.getTasks().stream().allMatch(t -> t.getWorkerInfo() != null);
        int parallelism = getExecutionGroupParallelism(cycle.getVertexGroup());
        if (!isWorkerAssigned && this.workers.get(cycle.getSchedulerId()) == null) {
            init(cycle);
            if (parallelism > this.workers.get(cycle.getSchedulerId()).getWorkers().size()) {
                return Collections.emptyList();
            }
        }
        List<WorkerInfo> workers = new ArrayList<>();
        List<WorkerInfo> workerInfos = this.workers.get(cycle.getSchedulerId()).getWorkers();
        String graphName = getGraphViewName(cycle);
        List<Integer> taskIndexes = new ArrayList<>();
        for (int i = 0; i < workerInfos.size(); i++) {
            taskIndexes.add(i);
        }
        Map<Integer, WorkerInfo> taskIndex2Worker = taskAssigner.assignTasks2Workers(graphName,
                taskIndexes, workerInfos);
        for (int i = 0; i < parallelism; i++) {
            WorkerInfo worker = assignTaskWorker(i, cycle,
                    cycle.getVertexGroup().getCycleGroupMeta().getAffinityLevel(),
                    taskIndex2Worker);
            workers.add(worker);
        }
        cycle.setWorkerAssigned(isAssigned.getOrDefault(cycle.getSchedulerId(), false));
        this.assigned.put(cycle.getSchedulerId(), workers);
        return workers;
    }

    @Override
    public void release(ExecutionNodeCycle cycle) {
        List<WorkerInfo> workerInfos = this.workers.get(cycle.getSchedulerId()).getWorkers();
        for (int i = 0, size = cycle.getTasks().size(); i < size; i++) {
            workerInfos.add(cycle.getTasks().get(i).getWorkerInfo());
        }
        LOGGER.info("current workers {}", this.workers.get(cycle.getSchedulerId()));
    }

    private WorkerInfo assignTaskWorker(int taskIndex, ExecutionNodeCycle cycle,
                                        AffinityLevel affinityLevel,
                                        Map<Integer, WorkerInfo> assignInfo) {
        switch (affinityLevel) {
            case worker:
                ExecutionTask task = cycle.getTasks().get(taskIndex);
                WorkerInfo workerInfo = assignInfo.get(taskIndex);
                if (workerInfo == null && task.getWorkerInfo() != null) {
                    workerInfo =  task.getWorkerInfo();
                } else {
                    task.setWorkerInfo(workerInfo);
                }
                return workerInfo;
            default:
                throw new GeaflowRuntimeException(
                        "not support affinity level yet " + affinityLevel);
        }
    }

    private String getGraphViewName(ExecutionNodeCycle cycle) {
        List<ExecutionTask> tasks = cycle.getTasks();
        for (ExecutionTask task : tasks) {
            AbstractProcessor processor = (AbstractProcessor) (task.getProcessor());
            if (processor != null) {
                Operator operator = processor.getOperator();
                if (operator instanceof AbstractGraphVertexCentricOp) {
                    AbstractGraphVertexCentricOp graphOp =
                            (AbstractGraphVertexCentricOp) operator;
                    return graphOp.getGraphViewName();
                }
            }
        }
        return DEFAULT_GRAPH_VIEW_NAME;
    }

}
