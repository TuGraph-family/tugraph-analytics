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

import com.antgroup.geaflow.cluster.common.ExecutionIdGenerator;
import com.antgroup.geaflow.cluster.common.IDispatcher;
import com.antgroup.geaflow.cluster.common.IEventListener;
import com.antgroup.geaflow.cluster.protocol.EventType;
import com.antgroup.geaflow.cluster.protocol.ICycleResponseEvent;
import com.antgroup.geaflow.cluster.protocol.IEvent;
import com.antgroup.geaflow.cluster.resourcemanager.WorkerInfo;
import com.antgroup.geaflow.cluster.response.IResult;
import com.antgroup.geaflow.cluster.rpc.RpcClient;
import com.antgroup.geaflow.common.exception.GeaflowDispatchException;
import com.antgroup.geaflow.common.exception.GeaflowRuntimeException;
import com.antgroup.geaflow.common.metric.PipelineMetrics;
import com.antgroup.geaflow.common.utils.FutureUtil;
import com.antgroup.geaflow.common.utils.LoggerFormatter;
import com.antgroup.geaflow.core.graph.ExecutionEdge;
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
import com.antgroup.geaflow.runtime.core.scheduler.result.IExecutionResult;
import com.antgroup.geaflow.runtime.core.scheduler.strategy.IScheduleStrategy;
import com.antgroup.geaflow.runtime.core.scheduler.strategy.TopologicalOrderScheduleStrategy;
import com.antgroup.geaflow.shuffle.memory.ShuffleDataManager;
import com.antgroup.geaflow.shuffle.message.PipelineInfo;
import com.antgroup.geaflow.shuffle.message.ShuffleId;
import com.antgroup.geaflow.shuffle.service.IShuffleMaster;
import com.antgroup.geaflow.shuffle.service.ShuffleManager;
import com.antgroup.geaflow.stats.collector.StatsCollectorFactory;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExecutionGraphCycleScheduler<R> extends AbstractCycleScheduler implements IEventListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(ExecutionGraphCycleScheduler.class);

    private static final int SCHEDULE_INTERVAL_MS = 1;

    private GraphCycleEventDispatcher dispatcher;
    private CycleResultManager resultManager;
    private Set<Integer> finishedCycles;
    // Current unfinished clean env event for certain iteration.
    private CountDownLatch cleanEnvWaitingResponse;
    private long sourceCycleNum;

    private long pipelineId;
    private String pipelineName;
    private PipelineMetrics pipelineMetrics;
    private List<IResult> results;

    @Override
    public void init(ICycleSchedulerContext context) {
        super.init(context);
        this.dispatcher = new GraphCycleEventDispatcher();
        this.resultManager = context.getResultManager();
        this.finishedCycles = new HashSet<>();
        this.results = new ArrayList<>();
    }

    @Override
    public void execute(long iterationId) {
        IScheduleStrategy scheduleStrategy = new TopologicalOrderScheduleStrategy(context.getConfig());
        scheduleStrategy.init(cycle);
        pipelineId = ExecutionIdGenerator.getInstance().generateId();
        pipelineName = getPipelineName(iterationId);
        this.sourceCycleNum = getSourceCycleNum(cycle);
        dispatcher.registerListener(cycle.getCycleId(), new ExecutionGraphCycleEventListener());
        LOGGER.info("{} execute iterationId {}, executionId {}", pipelineName, iterationId, pipelineId);
        this.pipelineMetrics = new PipelineMetrics(pipelineName);
        this.pipelineMetrics.setStartTime(System.currentTimeMillis());
        StatsCollectorFactory.getInstance().getPipelineStatsCollector().reportPipelineMetrics(pipelineMetrics);

        while (scheduleStrategy.hasNext()) {

            // Get cycle that ready to schedule.
            IExecutionCycle nextCycle = (IExecutionCycle) scheduleStrategy.next();
            if (nextCycle == null) {
                try {
                    Thread.sleep(SCHEDULE_INTERVAL_MS);
                } catch (InterruptedException e) {
                    throw new GeaflowRuntimeException(e);
                }
                continue;
            }

            ExecutionNodeCycle cycle = (ExecutionNodeCycle) nextCycle;
            cycle.setPipelineName(pipelineName);
            cycle.setPipelineId(pipelineId);
            LOGGER.info("{} start schedule {}, total task num {}, head task num {}, tail "
                    + "task num {} type:{}",
                pipelineName, LoggerFormatter.getCycleName(cycle.getCycleId(), iterationId), cycle.getTasks().size(),
                cycle.getCycleHeads().size(), cycle.getCycleTails().size(), cycle.getType());

            // Schedule cycle.
            ICycleScheduler cycleScheduler = CycleSchedulerFactory.create(cycle);
            ICycleSchedulerContext cycleContext = CycleSchedulerContextFactory.create(cycle, context);
            cycleScheduler.init(cycleContext);

            if (cycleScheduler instanceof IEventListener) {
                dispatcher.registerListener(cycle.getCycleId(), (IEventListener) cycleScheduler);
            }

            try {
                final long start = System.currentTimeMillis();
                IExecutionResult result = cycleScheduler.execute();
                if (!result.isSuccess()) {
                    throw new GeaflowRuntimeException(String.format("%s schedule execute %s failed ",
                        pipelineName, cycle.getCycleId())) ;
                }
                scheduleStrategy.finish(cycle);
                LOGGER.info("{} schedule {} finished, cost {}ms",
                    pipelineName, LoggerFormatter.getCycleName(cycle.getCycleId(), iterationId),
                    System.currentTimeMillis() - start);
                if (cycleScheduler instanceof IEventListener) {
                    dispatcher.removeListener(cycle.getCycleId());
                }
            } catch (Throwable e) {
                throw new GeaflowRuntimeException(String.format("%s schedule iterationId %s failed ",
                    pipelineName, iterationId), e) ;
            } finally {
                context.release(cycle);
                cycleScheduler.close();
                // Clean cycle input shuffle data.
                cleanInputShuffleData(cycle);
            }
        }
    }

    @Override
    public void finish(long iterationId) {
        String cycleLogTag = LoggerFormatter.getCycleTag(pipelineName, cycle.getCycleId(), iterationId);
        // Clean shuffle data for all used workers.
        context.getSchedulerWorkerManager().clean(usedWorkers ->
            cleanEnv(usedWorkers, cycleLogTag, iterationId, false));
        // Clear last iteration shard meta.
        resultManager.clear();
        this.pipelineMetrics.setDuration(System.currentTimeMillis() - pipelineMetrics.getStartTime());
        StatsCollectorFactory.getInstance().getPipelineStatsCollector().reportPipelineMetrics(pipelineMetrics);
        LOGGER.info("{} finished {}", cycleLogTag, pipelineMetrics);
    }

    @Override
    protected R finish() {
        // Clean shuffle data for all used workers.
        String cycleLogTag = LoggerFormatter.getCycleTag(pipelineName, cycle.getCycleId());
        context.getSchedulerWorkerManager().clean(usedWorkers -> cleanEnv(usedWorkers, cycleLogTag,
            cycle.getIterationCount(), true));
        return (R) results;
    }

    private void cleanEnv(Set<WorkerInfo> usedWorkers,
                          String cycleLogTag,
                          long iterationId,
                          boolean needCleanWorkerContext) {
        cleanEnvWaitingResponse = new CountDownLatch(usedWorkers.size());
        LOGGER.info("{} start wait {} clean env response for iteration {}, need clean worker context {}",
            cycleLogTag, usedWorkers.size(), iterationId, needCleanWorkerContext);
        List<Future<IEvent>> submitFutures = new ArrayList<>(usedWorkers.size());
        for (WorkerInfo worker : usedWorkers) {
            IEvent cleanEvent;
            if (needCleanWorkerContext) {
                cleanEvent = new CleanEnvEvent(worker.getWorkerIndex(),
                    cycle.getCycleId(), iterationId, pipelineId, cycle.getDriverId());
            } else {
                cleanEvent = new CleanStashEnvEvent(worker.getWorkerIndex(),
                    cycle.getCycleId(), iterationId, pipelineId, cycle.getDriverId());
            }
            Future<IEvent> future = RpcClient.getInstance()
                .processContainer(worker.getContainerName(), cleanEvent);
            submitFutures.add(future);
        }
        FutureUtil.wait(submitFutures);

        try {
            cleanEnvWaitingResponse.await();
        } catch (InterruptedException e) {
            throw new GeaflowRuntimeException("exception when wait all clean event finish", e);
        } finally {
            IShuffleMaster shuffleMaster = ShuffleManager.getInstance().getShuffleMaster();
            if (shuffleMaster != null) {
                shuffleMaster.clean(new PipelineInfo(pipelineId, pipelineName));
            }
            ShuffleDataManager.getInstance().release(pipelineId);
        }
    }

    private String getPipelineName(long iterationId) {
        return String.format("%s-%s", cycle.getPipelineName(), iterationId);
    }

    @Override
    public void close() {
        context.close();
        LOGGER.info("{} closed", cycle.getPipelineName());
    }

    @Override
    public void handleEvent(IEvent event) {
        dispatcher.dispatch(event);
    }

    private void cleanInputShuffleData(ExecutionNodeCycle cycle) {
        List<Integer> headVertexIds = cycle.getVertexGroup().getHeadVertexIds();
        for (int vid : headVertexIds) {
            List<Integer> edgeIds = cycle.getVertexGroup().getVertexId2InEdgeIds().get(vid);
            for (int eid : edgeIds) {
                ExecutionEdge edge = cycle.getVertexGroup().getEdgeMap().get(eid);
                ShuffleId shuffleId = new ShuffleId(cycle.getPipelineName(), edge.getSrcId(), eid);
                IShuffleMaster shuffleMaster = ShuffleManager.getInstance().getShuffleMaster();
                if (shuffleMaster != null) {
                    shuffleMaster.clean(shuffleId);
                }
            }
        }
    }

    private long getSourceCycleNum(IExecutionCycle cycle) {
        return ((ExecutionGraphCycle) cycle).getCycleMap().values().stream()
            .filter(e -> ((ExecutionNodeCycle) e).getVertexGroup().getParentVertexGroupIds().isEmpty()).count();
    }

    private class GraphCycleEventDispatcher implements IDispatcher {

        private Map<Integer, IEventListener> cycleIdToScheduler;

        public GraphCycleEventDispatcher() {
            this.cycleIdToScheduler = new ConcurrentHashMap<>();
        }

        @Override
        public void dispatch(IEvent event) throws GeaflowDispatchException {
            if (event instanceof ICycleResponseEvent) {
                ICycleResponseEvent callbackEvent = (ICycleResponseEvent) event;

                if (cycleIdToScheduler.containsKey(callbackEvent.getCycleId())) {
                    cycleIdToScheduler.get(callbackEvent.getCycleId()).handleEvent(event);
                }
            }
        }

        public void registerListener(int cycleId, IEventListener eventListener) {
            cycleIdToScheduler.put(cycleId, eventListener);
        }

        public void removeListener(int cycleId) {
            cycleIdToScheduler.remove(cycleId);

        }
    }

    private class ExecutionGraphCycleEventListener implements IEventListener {

        @Override
        public void handleEvent(IEvent event) {
            switch (event.getEventType()) {
                case DONE:
                    DoneEvent doneEvent = (DoneEvent) event;
                    switch (doneEvent.getSourceEvent()) {
                        case LAUNCH_SOURCE:
                            if (!finishedCycles.contains(doneEvent.getResult())) {
                                finishedCycles.add((Integer) doneEvent.getResult());
                                LOGGER.info("{} cycle {} source finished at iteration {}",
                                    cycle.getPipelineName(), doneEvent.getResult(), doneEvent.getWindowId());
                                if (finishedCycles.size() == sourceCycleNum) {
                                    ((AbstractCycleSchedulerContext) context).setTerminateIterationId(
                                        doneEvent.getWindowId());
                                    LOGGER.info("{} all source cycle finished", cycle.getPipelineName());
                                }
                            }
                            break;
                        case CLEAN_ENV:
                            if (event.getEventType() == EventType.DONE) {
                                cleanEnvWaitingResponse.countDown();
                            }
                            LOGGER.info("{} received clean env event {}",
                                getPipelineName(doneEvent.getWindowId()), cleanEnvWaitingResponse.getCount());
                            break;
                        default:
                            throw new GeaflowRuntimeException(String.format("%s not support handle done event %s",
                                getPipelineName(doneEvent.getWindowId()), event));

                    }
                    break;
                default:
                    throw new GeaflowRuntimeException(String.format("%s not support handle event %s",
                        cycle.getPipelineName(), event));
            }
        }
    }
}
