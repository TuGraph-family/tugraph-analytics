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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
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
    }

    @Override
    public void execute(long iterationId) {

        IScheduleStrategy scheduleStrategy = new TopologicalOrderScheduleStrategy(context.getConfig());
        scheduleStrategy.init(cycle);
        pipelineId = ExecutionIdGenerator.getInstance().generateId();
        pipelineName = getPipelineName(iterationId);
        cycleLogTag = pipelineName;
        this.sourceCycleNum = getSourceCycleNum(cycle);
        dispatcher.registerListener(cycle.getCycleId(), new ExecutionGraphCycleEventListener());
        LOGGER.info("{} execute iterationId {}, executionId {}", cycleLogTag, iterationId, pipelineId);
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
            LOGGER.info("{} start schedule cycle {}, total task num {}, head task num {}, tail "
                    + "task num {} type:{}",
                cycleLogTag, cycle.getCycleId(), cycle.getTasks().size(),
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
                    throw new GeaflowRuntimeException(String.format("schedule execute %s failed ",
                        getPipelineName(iterationId))) ;
                }
                scheduleStrategy.finish(cycle);
                LOGGER.info("{} iterationId {} finished, cost {}ms",
                    cycleLogTag, iterationId, System.currentTimeMillis() - start);
                if (cycleScheduler instanceof IEventListener) {
                    dispatcher.removeListener(cycle.getCycleId());
                }
            } catch (Throwable e) {
                throw new GeaflowRuntimeException(String.format("%s schedule iterationId %s failed ",
                    cycleLogTag, iterationId), e) ;
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
        LOGGER.info("{} finish", cycleLogTag);
        // Clean shuffle data for all used workers.
        context.getSchedulerWorkerManager().clean(usedWorkers -> cleanEnv(usedWorkers, iterationId, false));
        // Clear last iteration shard meta.
        resultManager.clear();
        this.pipelineMetrics.setDuration(System.currentTimeMillis() - pipelineMetrics.getStartTime());
        StatsCollectorFactory.getInstance().getPipelineStatsCollector().reportPipelineMetrics(pipelineMetrics);
        LOGGER.info("{} iterationId {} clean and finish done, {}", pipelineName, iterationId, pipelineMetrics);
    }

    @Override
    protected R finish() {
        // Clean shuffle data for all used workers.
        context.getSchedulerWorkerManager().clean(usedWorkers -> cleanEnv(usedWorkers, cycle.getIterationCount(), true));
        return (R) results;
    }

    private void cleanEnv(Set<WorkerInfo> usedWorkers, long iterationId, boolean needCleanWorkerContext) {
        cleanEnvWaitingResponse = new CountDownLatch(usedWorkers.size());
        LOGGER.info("{} start wait {} clean env response for iteration {}, need clean worker context {}",
            cycleLogTag, usedWorkers.size(), iterationId, needCleanWorkerContext);
        for (WorkerInfo worker : usedWorkers) {
            IEvent cleanEvent;
            if (needCleanWorkerContext) {
                cleanEvent = new CleanEnvEvent(worker.getWorkerIndex(),
                    cycle.getCycleId(), iterationId, pipelineId, cycle.getDriverId());
            } else {
                cleanEvent = new CleanStashEnvEvent(worker.getWorkerIndex(),
                    cycle.getCycleId(), iterationId, pipelineId, cycle.getDriverId());
            }
            RpcClient.getInstance().processContainer(worker.getContainerName(), cleanEvent);
        }

        try {
            cleanEnvWaitingResponse.await();
        } catch (InterruptedException e) {
            throw new GeaflowRuntimeException("exception when wait all clean event finish", e);
        } finally {
            LOGGER.info("{} clean shuffle master", cycleLogTag);
            IShuffleMaster shuffleMaster = ShuffleManager.getInstance().getShuffleMaster();
            if (shuffleMaster != null) {
                shuffleMaster.clean(new PipelineInfo(pipelineId, pipelineName));
            }
            ShuffleDataManager.getInstance().release(pipelineId);
        }
    }

    private String getPipelineName(long iterationId) {
        if (cycle.getIterationCount() > 1) {
            return String.format("%s-%s", cycle.getPipelineName(), iterationId);
        } else {
            return cycle.getPipelineName();
        }
    }

    @Override
    public void close() {
        context.close();
        LOGGER.info("{} scheduler closed", cycleLogTag);
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
            LOGGER.info("{} register scheduler {}", cycle.getPipelineName(), cycleId);
            cycleIdToScheduler.put(cycleId, eventListener);
        }

        public void removeListener(int cycleId) {
            LOGGER.info("{} remove scheduler {}", cycle.getPipelineName(), cycleId);
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
                                LOGGER.info("cycle {} source finished at iteration {}",
                                    doneEvent.getResult(), doneEvent.getWindowId());
                                if (finishedCycles.size() == sourceCycleNum) {
                                    ((AbstractCycleSchedulerContext) context).setTerminateIterationId(
                                        doneEvent.getWindowId());
                                    LOGGER.info("all source cycle finished");
                                }
                            }
                            break;
                        case CLEAN_ENV:
                            if (event.getEventType() == EventType.DONE) {
                                cleanEnvWaitingResponse.countDown();
                            }
                            LOGGER.info("received clean env event {}", cleanEnvWaitingResponse.getCount());
                            break;
                        default:
                            throw new GeaflowRuntimeException(String.format("%s not support handle done event %s",
                                cycleLogTag, event));

                    }
                    break;
                default:
                    throw new GeaflowRuntimeException(String.format("%s not support handle event %s",
                        cycleLogTag, event));
            }
        }
    }
}
