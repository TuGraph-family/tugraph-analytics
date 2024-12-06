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

import com.antgroup.geaflow.cluster.resourcemanager.ReleaseResourceRequest;
import com.antgroup.geaflow.cluster.resourcemanager.ResourceInfo;
import com.antgroup.geaflow.cluster.rpc.RpcClient;
import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.exception.GeaflowRuntimeException;
import com.antgroup.geaflow.ha.runtime.HighAvailableLevel;
import com.antgroup.geaflow.runtime.core.scheduler.cycle.ExecutionCycleType;
import com.antgroup.geaflow.runtime.core.scheduler.cycle.ExecutionGraphCycle;
import com.antgroup.geaflow.runtime.core.scheduler.cycle.IExecutionCycle;
import com.google.common.annotations.VisibleForTesting;
import java.util.Map;

public class ScheduledWorkerManagerFactory {

    private static volatile RedoCycleScheduledWorkerManager redoWorkerManager;
    private static volatile CheckpointCycleScheduledWorkerManager checkpointWorkerManager;

    public static <WM extends IScheduledWorkerManager<ExecutionGraphCycle>> WM createScheduledWorkerManager(
        Configuration config, HighAvailableLevel level) {
        switch (level) {
            case REDO:
                if (redoWorkerManager == null) {
                    synchronized (ScheduledWorkerManagerFactory.class) {
                        if (redoWorkerManager == null) {
                            redoWorkerManager = new RedoCycleScheduledWorkerManager(config);
                        }
                    }
                }
                return (WM) redoWorkerManager;
            case CHECKPOINT:
                if (checkpointWorkerManager == null) {
                    synchronized (ScheduledWorkerManagerFactory.class) {
                        if (checkpointWorkerManager == null) {
                            checkpointWorkerManager = new CheckpointCycleScheduledWorkerManager(config);
                        }
                    }
                }
                return (WM) checkpointWorkerManager;
            default:
                throw new GeaflowRuntimeException("not support worker manager type " + level);
        }

    }

    @VisibleForTesting
    public static synchronized void clear() {
        if (redoWorkerManager != null) {
            clear(redoWorkerManager);
            redoWorkerManager = null;
        }
        if (checkpointWorkerManager != null) {
            clear(checkpointWorkerManager);
            checkpointWorkerManager = null;
        }
    }

    private static void clear(AbstractScheduledWorkerManager workerManager) {
        if (workerManager.workers != null) {
            for (Map.Entry<Long, ResourceInfo> workerEntry : workerManager.workers.entrySet()) {
                RpcClient.getInstance().releaseResource(workerManager.masterId,
                    ReleaseResourceRequest.build(workerEntry.getValue().getResourceId(), workerEntry.getValue().getWorkers()));
            }
        }
    }

    public static HighAvailableLevel getWorkerManagerHALevel(IExecutionCycle cycle) {
        if (cycle.getType() == ExecutionCycleType.GRAPH) {
            ExecutionGraphCycle graph = (ExecutionGraphCycle) cycle;
            if (graph.getHighAvailableLevel() == HighAvailableLevel.CHECKPOINT) {
                return HighAvailableLevel.CHECKPOINT;
            }
            // As for stream case, the whole graph is REDO ha level while child cycle is CHECKPOINT.
            // We need set worker manager ha level to CHECKPOINT
            // to make sure all request worker initialized with CHECKPOINT level.
            if (graph.getCycleMap().size() == 1) {
                IExecutionCycle child = graph.getCycleMap().values().iterator().next();
                if (child.getHighAvailableLevel() == HighAvailableLevel.CHECKPOINT) {
                    return HighAvailableLevel.CHECKPOINT;
                }
            }
            return HighAvailableLevel.REDO;

        } else {
            return cycle.getHighAvailableLevel();
        }
    }
}
