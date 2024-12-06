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

import com.antgroup.geaflow.cluster.common.IEventListener;
import com.antgroup.geaflow.cluster.protocol.EventType;
import com.antgroup.geaflow.cluster.protocol.IEvent;
import com.antgroup.geaflow.cluster.resourcemanager.WorkerInfo;
import com.antgroup.geaflow.cluster.rpc.RpcClient;
import com.antgroup.geaflow.common.exception.GeaflowRuntimeException;
import com.antgroup.geaflow.common.metric.PipelineMetrics;
import com.antgroup.geaflow.common.utils.FutureUtil;
import com.antgroup.geaflow.common.utils.LoggerFormatter;
import com.antgroup.geaflow.runtime.core.protocol.CleanEnvEvent;
import com.antgroup.geaflow.runtime.core.protocol.CleanStashEnvEvent;
import com.antgroup.geaflow.runtime.core.protocol.DoneEvent;
import com.antgroup.geaflow.runtime.core.scheduler.context.AbstractCycleSchedulerContext;
import com.antgroup.geaflow.runtime.core.scheduler.context.CycleSchedulerContextFactory;
import com.antgroup.geaflow.runtime.core.scheduler.context.ICycleSchedulerContext;
import com.antgroup.geaflow.runtime.core.scheduler.cycle.ExecutionGraphCycle;
import com.antgroup.geaflow.runtime.core.scheduler.cycle.ExecutionNodeCycle;
import com.antgroup.geaflow.runtime.core.scheduler.cycle.IExecutionCycle;
import com.antgroup.geaflow.runtime.core.scheduler.io.CycleResultManager;
import com.antgroup.geaflow.runtime.core.scheduler.io.DataExchanger;
import com.antgroup.geaflow.runtime.core.scheduler.response.ComputeFinishEventListener;
import com.antgroup.geaflow.runtime.core.scheduler.response.EventListenerKey;
import com.antgroup.geaflow.runtime.core.scheduler.response.SourceFinishResponseEventListener;
import com.antgroup.geaflow.runtime.core.scheduler.result.IExecutionResult;
import com.antgroup.geaflow.runtime.core.scheduler.statemachine.IScheduleState;
import com.antgroup.geaflow.runtime.core.scheduler.statemachine.ScheduleState;
import com.antgroup.geaflow.runtime.core.scheduler.statemachine.graph.GraphStateMachine;
import com.antgroup.geaflow.runtime.core.scheduler.strategy.IScheduleStrategy;
import com.antgroup.geaflow.runtime.core.scheduler.strategy.TopologicalOrderScheduleStrategy;
import com.antgroup.geaflow.shuffle.memory.ShuffleDataManager;
import com.antgroup.geaflow.stats.collector.StatsCollectorFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExecutionGraphCycleScheduler<PC extends IExecutionCycle, PCC extends ICycleSchedulerContext<PC, ?, ?>, R, E>
    extends AbstractCycleScheduler<ExecutionGraphCycle, PC, PCC, R, E> implements IEventListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(ExecutionGraphCycleScheduler.class);

    private CycleResultManager resultManager;
    private long pipelineId;
    private long schedulerId;
    private String pipelineName;
    private PipelineMetrics pipelineMetrics;
    private List<Object> results;
    private String cycleLogTag;

    public ExecutionGraphCycleScheduler() {
    }

    public ExecutionGraphCycleScheduler(long schedulerId) {
        this.schedulerId = schedulerId;
    }

    @Override
    public void init(ICycleSchedulerContext<ExecutionGraphCycle, PC, PCC> context) {
        super.init(context);
        this.resultManager = context.getResultManager();
        this.results = new ArrayList<>();
        this.context.getSchedulerWorkerManager().init(this.cycle);
        this.context.getSchedulerWorkerManager().assign(this.cycle);
        this.pipelineId = this.cycle.getPipelineId();
        this.cycleLogTag = LoggerFormatter.getCycleTag(context.getCycle().getPipelineName(), cycle.getCycleId());
        this.dispatcher = new SchedulerEventDispatcher(cycleLogTag);
        registerEventListener();
        this.stateMachine = new GraphStateMachine();
        this.stateMachine.init(context);
    }

    @Override
    public void execute(IScheduleState event) {
        ScheduleState state = (ScheduleState) event;
        switch (state.getScheduleStateType()) {
            case EXECUTE_COMPUTE:
                execute(context.getNextIterationId());
                break;
            default:
                throw new GeaflowRuntimeException(String.format("not support event {}", event));
        }
    }

    public void execute(long iterationId) {
        IScheduleStrategy<ExecutionGraphCycle, IExecutionCycle> scheduleStrategy = new TopologicalOrderScheduleStrategy(context.getConfig());
        scheduleStrategy.init(cycle);
        this.pipelineName = getPipelineName(iterationId);
        this.pipelineMetrics = new PipelineMetrics(pipelineName);
        this.pipelineMetrics.setStartTime(System.currentTimeMillis());
        StatsCollectorFactory.getInstance().getPipelineStatsCollector().reportPipelineMetrics(pipelineMetrics);
        LOGGER.info("{} execute iterationId {}, executionId {}", pipelineName, iterationId, pipelineId);

        while (scheduleStrategy.hasNext()) {

            // Get cycle that ready to schedule.
            IExecutionCycle nextCycle = scheduleStrategy.next();
            ExecutionNodeCycle cycle = (ExecutionNodeCycle) nextCycle;
            cycle.setPipelineName(pipelineName);
            LOGGER.info("{} start schedule {}, total task num {}, head task num {}, tail "
                    + "task num {} type:{}",
                pipelineName, LoggerFormatter.getCycleName(cycle.getCycleId(), iterationId), cycle.getTasks().size(),
                cycle.getCycleHeads().size(), cycle.getCycleTails().size(), cycle.getType());

            // Schedule cycle.
            PipelineCycleScheduler cycleScheduler =
                (PipelineCycleScheduler) CycleSchedulerFactory.create(cycle);
            ICycleSchedulerContext cycleContext = CycleSchedulerContextFactory.create(cycle, context);
            cycleScheduler.init(cycleContext);

            EventListenerKey listenerKey = EventListenerKey.of(cycle.getCycleId());
            if (cycleScheduler instanceof IEventListener) {
                dispatcher.registerListener(listenerKey, cycleScheduler);
            }

            try {
                final long start = System.currentTimeMillis();
                IExecutionResult result = cycleScheduler.execute();
                if (!result.isSuccess()) {
                    throw new GeaflowRuntimeException(String.format("%s schedule execute %s failed ",
                        pipelineName, cycle.getCycleId())) ;
                }
                if (result.getResult() != null) {
                    results.add(result.getResult());
                }
                scheduleStrategy.finish(cycle);
                LOGGER.info("{} schedule {} finished, cost {}ms",
                    pipelineName, LoggerFormatter.getCycleName(cycle.getCycleId(), iterationId),
                    System.currentTimeMillis() - start);
                if (cycleScheduler instanceof IEventListener) {
                    dispatcher.removeListener(listenerKey);
                }
            } catch (Throwable e) {
                throw new GeaflowRuntimeException(String.format("%s schedule iterationId %s failed ",
                    pipelineName, iterationId), e) ;
            } finally {
                cycleScheduler.close();
            }
        }
    }

    @Override
    public void finish(long iterationId) {
        String cycleLogTag = LoggerFormatter.getCycleTag(pipelineName, cycle.getCycleId(), iterationId);
        // Clean shuffle data for all used workers.
        context.getSchedulerWorkerManager().clean(usedWorkers ->
            cleanEnv(usedWorkers, cycleLogTag, iterationId, false), cycle);
        // Clear last iteration shard meta.
        resultManager.clear();
        DataExchanger.clear();
        this.pipelineMetrics.setDuration(System.currentTimeMillis() - pipelineMetrics.getStartTime());
        StatsCollectorFactory.getInstance().getPipelineStatsCollector().reportPipelineMetrics(pipelineMetrics);
        LOGGER.info("{} finished {}", cycleLogTag, pipelineMetrics);
    }

    @Override
    protected R finish() {
        // Clean shuffle data for all used workers.
        String cycleLogTag = LoggerFormatter.getCycleTag(pipelineName, cycle.getCycleId());
        context.getSchedulerWorkerManager().clean(usedWorkers -> cleanEnv(usedWorkers, cycleLogTag,
            cycle.getIterationCount(), true), cycle);
        return (R) results;
    }

    private void cleanEnv(List<WorkerInfo> usedWorkers,
                          String cycleLogTag,
                          long iterationId,
                          boolean needCleanWorkerContext) {
        CountDownLatch latch = new CountDownLatch(1);
        LOGGER.info("{} start wait {} clean env response, need clean worker context {}",
            cycleLogTag, usedWorkers.size(), needCleanWorkerContext);
        // Register listener to handle response.
        EventListenerKey listenerKey = EventListenerKey.of(cycle.getCycleId(), EventType.CLEAN_ENV);
        ComputeFinishEventListener listener =
            new ComputeFinishEventListener(usedWorkers.size(), events -> {
                LOGGER.info("{} clean env response {} finished all {} events", cycleLogTag, listenerKey, events.size());
                latch.countDown();
            });
        dispatcher.registerListener(listenerKey, listener);

        List<Future<IEvent>> submitFutures = new ArrayList<>(usedWorkers.size());
        for (WorkerInfo worker : usedWorkers) {
            IEvent cleanEvent;
            if (needCleanWorkerContext) {
                cleanEvent = new CleanEnvEvent(schedulerId, worker.getWorkerIndex(),
                    cycle.getCycleId(), iterationId, pipelineId, cycle.getDriverId());
            } else {
                cleanEvent = new CleanStashEnvEvent(schedulerId, worker.getWorkerIndex(),
                    cycle.getCycleId(), iterationId, pipelineId, cycle.getDriverId());
            }
            Future<IEvent> future = RpcClient.getInstance()
                .processContainer(worker.getContainerName(), cleanEvent);
            submitFutures.add(future);
        }
        FutureUtil.wait(submitFutures);

        try {
            latch.await();
        } catch (InterruptedException e) {
            throw new GeaflowRuntimeException("exception when wait all clean event finish", e);
        } finally {
            ShuffleDataManager.getInstance().release(pipelineId);
        }
    }

    private String getPipelineName(long iterationId) {
        return String.format("%s-%s", cycle.getPipelineName(), iterationId);
    }

    @Override
    public void close() {
        context.getSchedulerWorkerManager().release(this.cycle);
        context.close(this.cycle);
        LOGGER.info("{} closed", cycle.getPipelineName());
    }

    public long getSchedulerId() {
        return schedulerId;
    }

    @Override
    public void handleEvent(IEvent event) {
        dispatcher.dispatch(event);
    }

    protected void registerEventListener() {
        EventListenerKey listenerKey = EventListenerKey.of(context.getCycle().getCycleId(), EventType.LAUNCH_SOURCE);
        IEventListener listener =
            new SourceFinishResponseEventListener(getSourceCycleNum(cycle),
                events -> {
                    long finishWindowId = ((DoneEvent) events.iterator().next()).getWindowId();
                    LOGGER.info("{} all source finished at {}", cycleLogTag, finishWindowId);
                    ((AbstractCycleSchedulerContext) context).setTerminateIterationId(
                        ((DoneEvent) events.iterator().next()).getWindowId());
                });
        // Register listener for end of source event.
        this.dispatcher.registerListener(listenerKey, listener);
    }

    private int getSourceCycleNum(IExecutionCycle cycle) {
        return (int) ((ExecutionGraphCycle) cycle).getCycleMap().values().stream()
            .filter(e -> ((ExecutionNodeCycle) e).getVertexGroup().getParentVertexGroupIds().isEmpty()).count();
    }

}
