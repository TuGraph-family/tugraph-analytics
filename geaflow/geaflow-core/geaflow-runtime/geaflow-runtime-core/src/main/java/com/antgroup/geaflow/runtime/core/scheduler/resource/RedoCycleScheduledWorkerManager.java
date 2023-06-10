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
import com.antgroup.geaflow.runtime.core.scheduler.cycle.ExecutionNodeCycle;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class RedoCycleScheduledWorkerManager extends AbstractScheduledWorkerManager {

    public RedoCycleScheduledWorkerManager(Configuration config) {
        super(config);
    }

    public List<WorkerInfo> assign(ExecutionNodeCycle cycle) {
        int parallelism = getExecutionGroupParallelism(cycle.getVertexGroup());
        if (parallelism > available.size()) {
            return Collections.emptyList();
        }
        List<WorkerInfo> workers = new ArrayList<>();
        for (int i = 0; i < parallelism; i++) {
            cycle.getTasks().get(i).setWorkerInfo(this.available.get(i));
            workers.add(this.available.get(i));
        }
        cycle.setWorkerAssigned(false);
        assigned.addAll(workers);
        return workers;
    }

    @Override
    public void release(ExecutionNodeCycle vertex) {
        // Do nothing because workers are not removed when assigning.
    }

}
