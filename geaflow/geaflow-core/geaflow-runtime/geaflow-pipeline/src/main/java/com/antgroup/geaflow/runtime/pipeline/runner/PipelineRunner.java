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

package com.antgroup.geaflow.runtime.pipeline.runner;

import com.antgroup.geaflow.cluster.common.ExecutionIdGenerator;
import com.antgroup.geaflow.cluster.common.IEventListener;
import com.antgroup.geaflow.cluster.driver.DriverEventDispatcher;
import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.exception.GeaflowRuntimeException;
import com.antgroup.geaflow.core.graph.ExecutionGraph;
import com.antgroup.geaflow.core.graph.ExecutionTask;
import com.antgroup.geaflow.core.graph.builder.ExecutionGraphBuilder;
import com.antgroup.geaflow.pipeline.callback.TaskCallBack;
import com.antgroup.geaflow.plan.graph.PipelineGraph;
import com.antgroup.geaflow.runtime.core.scheduler.CycleSchedulerFactory;
import com.antgroup.geaflow.runtime.core.scheduler.ExecutionCycleTaskAssigner;
import com.antgroup.geaflow.runtime.core.scheduler.ICycleScheduler;
import com.antgroup.geaflow.runtime.core.scheduler.context.AbstractCycleSchedulerContext;
import com.antgroup.geaflow.runtime.core.scheduler.context.CycleSchedulerContextFactory;
import com.antgroup.geaflow.runtime.core.scheduler.context.ICycleSchedulerContext;
import com.antgroup.geaflow.runtime.core.scheduler.cycle.ExecutionCycleBuilder;
import com.antgroup.geaflow.runtime.core.scheduler.cycle.IExecutionCycle;
import com.antgroup.geaflow.runtime.core.scheduler.result.IExecutionResult;
import com.antgroup.geaflow.runtime.pipeline.service.PipelineServiceExecutorContext;
import com.antgroup.geaflow.runtime.pipeline.task.PipelineTaskExecutorContext;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PipelineRunner {

    private static final Logger LOGGER = LoggerFactory.getLogger(PipelineRunner.class);

    private DriverEventDispatcher eventDispatcher;

    public PipelineRunner(DriverEventDispatcher eventDispatcher) {
        this.eventDispatcher = eventDispatcher;
    }

    public IExecutionResult executePipelineGraph(String name, String driverId,
                                                 Configuration config, TaskCallBack taskCallBack,
                                                 PipelineGraph pipelineGraph) {
        ICycleSchedulerContext context = CycleSchedulerContextFactory.build(() -> {
            ExecutionGraphBuilder builder = new ExecutionGraphBuilder(pipelineGraph);
            ExecutionGraph graph = builder.buildExecutionGraph(config);

            Map<Integer, List<ExecutionTask>> vertex2Tasks = ExecutionCycleTaskAssigner.assign(graph);

            IExecutionCycle cycle = ExecutionCycleBuilder.buildExecutionCycle(graph, vertex2Tasks,
                config, ExecutionIdGenerator.getInstance().generateId(), name, driverId);
            return CycleSchedulerContextFactory.create(cycle, null);
        });
        if (taskCallBack != null) {
            ((AbstractCycleSchedulerContext) context).setCallbackFunction(taskCallBack.getCallbackFunction());
        }

        ICycleScheduler scheduler = CycleSchedulerFactory.create(context.getCycle());
        if (scheduler instanceof IEventListener) {
            eventDispatcher.registerListener((IEventListener) scheduler);
        }

        scheduler.init(context);
        IExecutionResult result = scheduler.execute();
        LOGGER.info("final result of pipeline is {}", result.getResult());
        scheduler.close();
        return result;
    }

    public void runPipelineGraph(PipelineGraph pipelineGraph, TaskCallBack taskCallBack,
                                 PipelineTaskExecutorContext taskExecutorContext) {
        IExecutionResult result = executePipelineGraph(taskExecutorContext.getPipelineTaskName(),
            taskExecutorContext.getDriverId(), taskExecutorContext.getPipelineContext().getConfig(), 
            taskCallBack, pipelineGraph);
        if (!result.isSuccess()) {
            throw new GeaflowRuntimeException("run pipeline task failed, cause: " + result.getError());
        }
    }

    public void runPipelineGraph(PipelineGraph pipelineGraph,
                                 PipelineServiceExecutorContext serviceExecutorContext) {
        //TODO Service task callback.
        executePipelineGraph(serviceExecutorContext.getPipelineTaskName(),
            serviceExecutorContext.getDriverId(),
            serviceExecutorContext.getPipelineContext().getConfig(), null, pipelineGraph );
    }
}
