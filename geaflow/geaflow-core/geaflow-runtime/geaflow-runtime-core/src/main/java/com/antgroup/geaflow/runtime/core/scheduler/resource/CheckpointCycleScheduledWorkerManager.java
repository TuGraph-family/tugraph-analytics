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
import com.antgroup.geaflow.plan.graph.AffinityLevel;
import com.antgroup.geaflow.runtime.core.scheduler.cycle.ExecutionNodeCycle;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
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
        for (int i = 0; i < parallelism; i++) {
            WorkerInfo worker = assignTaskWorker(cycle.getTasks().get(i),
                cycle.getVertexGroup().getCycleGroupMeta().getAffinityLevel(), workerInfos);
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

    private WorkerInfo assignTaskWorker(ExecutionTask task, AffinityLevel affinityLevel, List<WorkerInfo> workInfos) {
        switch (affinityLevel) {
            case worker:
                WorkerInfo worker;
                if (task.getWorkerInfo() == null) {
                    worker = workInfos.remove(0);
                    task.setWorkerInfo(worker);
                } else {
                    worker = task.getWorkerInfo();
                    workInfos.remove(worker);
                }
                return worker;
            default:
                throw new GeaflowRuntimeException("not support affinity level yet " + affinityLevel);
        }
    }

}
