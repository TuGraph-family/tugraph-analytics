/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.antgroup.geaflow.runtime.core.protocol;

import com.antgroup.geaflow.cluster.collector.CloseEmitterRequest;
import com.antgroup.geaflow.cluster.fetcher.CloseFetchRequest;
import com.antgroup.geaflow.cluster.protocol.EventType;
import com.antgroup.geaflow.cluster.task.ITaskContext;
import com.antgroup.geaflow.common.metric.EventMetrics;
import com.antgroup.geaflow.common.utils.GcUtil;
import com.antgroup.geaflow.core.graph.ExecutionTask;
import com.antgroup.geaflow.runtime.core.worker.context.AbstractWorkerContext;
import com.antgroup.geaflow.runtime.core.worker.context.WorkerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Clean worker runtime execution env, e.g. close shuffle reader/writer and close processor.
 * Reverse event of {@link InitCycleEvent}: {@link InitCycleEvent} for initialize env while {@link CleanCycleEvent} for clean env.
 */
public class CleanCycleEvent extends AbstractCleanCommand {

    private static final Logger LOGGER = LoggerFactory.getLogger(CleanCycleEvent.class);

    public CleanCycleEvent(long schedulerId, int workerId, int cycleId, long windowId) {
        super(schedulerId, workerId, cycleId, windowId);
    }

    @Override
    public void execute(ITaskContext taskContext) {
        super.execute(taskContext);
        WorkerContext workerContext = (WorkerContext) this.context;
        ExecutionTask executionTask = workerContext.getExecutionTask();
        this.fetcherRunner.add(new CloseFetchRequest(executionTask.getTaskId()));
        this.emitterRunner.add(new CloseEmitterRequest(executionTask.getTaskId(), this.windowId));
        this.worker.close();
        EventMetrics eventMetrics = ((AbstractWorkerContext) this.context).getEventMetrics();
        eventMetrics.setFinishTime(System.currentTimeMillis());
        eventMetrics.setFinishGcTs(GcUtil.computeCurrentTotalGcTime());
        LOGGER.info("clean task {} {}/{} of {} {} : {}",
            executionTask.getTaskId(),
            executionTask.getIndex(),
            executionTask.getParallelism(),
            executionTask.getVertexId(),
            executionTask.getProcessor().toString(),
            eventMetrics);
        this.sendDoneEvent(workerContext.getDriverId(), EventType.CLEAN_CYCLE, null, true);
    }

    @Override
    public EventType getEventType() {
        return EventType.CLEAN_CYCLE;
    }

    @Override
    public String toString() {
        return "CleanCycleEvent{"
            + "schedulerId=" + schedulerId
            + ", workerId=" + workerId
            + ", windowId=" + windowId
            + '}';
    }
}
