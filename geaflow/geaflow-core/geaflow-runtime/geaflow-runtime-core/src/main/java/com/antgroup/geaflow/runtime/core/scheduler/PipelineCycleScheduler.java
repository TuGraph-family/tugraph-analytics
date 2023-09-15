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

import static com.antgroup.geaflow.runtime.core.scheduler.io.IoDescriptorBuilder.COLLECT_DATA_EDGE_ID;

import com.antgroup.geaflow.cluster.common.IEventListener;
import com.antgroup.geaflow.cluster.protocol.EventType;
import com.antgroup.geaflow.cluster.protocol.IEvent;
import com.antgroup.geaflow.cluster.resourcemanager.WorkerInfo;
import com.antgroup.geaflow.cluster.response.IResult;
import com.antgroup.geaflow.cluster.rpc.RpcClient;
import com.antgroup.geaflow.common.exception.GeaflowRuntimeException;
import com.antgroup.geaflow.common.metric.CycleMetrics;
import com.antgroup.geaflow.common.shuffle.DataExchangeMode;
import com.antgroup.geaflow.common.utils.LoggerFormatter;
import com.antgroup.geaflow.core.graph.ExecutionTask;
import com.antgroup.geaflow.io.CollectType;
import com.antgroup.geaflow.runtime.core.protocol.ComposeEvent;
import com.antgroup.geaflow.runtime.core.protocol.DoneEvent;
import com.antgroup.geaflow.runtime.core.scheduler.context.AbstractCycleSchedulerContext;
import com.antgroup.geaflow.runtime.core.scheduler.context.ICycleSchedulerContext;
import com.antgroup.geaflow.runtime.core.scheduler.cycle.ExecutionCycleType;
import com.antgroup.geaflow.runtime.core.scheduler.cycle.ExecutionNodeCycle;
import com.antgroup.geaflow.runtime.core.scheduler.io.CycleResultManager;
import com.antgroup.geaflow.shuffle.memory.ShuffleDataManager;
import com.antgroup.geaflow.shuffle.service.ShuffleManager;
import com.antgroup.geaflow.stats.collector.StatsCollectorFactory;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is pipeline cycle scheduler impl.
 */
public class PipelineCycleScheduler<E>
    extends AbstractCycleScheduler<List<IResult>, E> implements IEventListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(PipelineCycleScheduler.class);

    private static final String FINISH_TAG = "FINISH";

    private ExecutionNodeCycle nodeCycle;
    private CycleResultManager resultManager;

    private boolean enableSchedulerDebug = false;
    private CycleResponseEventPool<IEvent> responseEventPool;
    private HashMap<Long, List<IEvent>> iterationIdToFinishedTasks;
    private HashMap<Long, CycleMetrics> iterationIdToMetrics;
    private Map<Integer, ExecutionTask> cycleTasks;
    private Map<Integer, Long> taskIdToFinishedSourceIds;
    private long pipelineId;
    private boolean isIteration;

    private DataExchangeMode inputExchangeMode;
    private DataExchangeMode outputExchangeMode;

    private SchedulerEventBuilder eventBuilder;
    private SchedulerGraphAggregateProcessor aggregator;

    public PipelineCycleScheduler() {
    }

    @Override
    public void init(ICycleSchedulerContext context) {
        super.init(context);
        this.responseEventPool = new CycleResponseEventPool<>();
        this.iterationIdToFinishedTasks = new HashMap<>();
        this.iterationIdToMetrics = new HashMap<>();
        this.taskIdToFinishedSourceIds = new HashMap<>();
        this.nodeCycle = (ExecutionNodeCycle) context.getCycle();
        this.cycleLogTag = LoggerFormatter.getCycleTag(nodeCycle.getPipelineName(), nodeCycle.getCycleId());
        this.cycleTasks = nodeCycle.getTasks().stream().collect(Collectors.toMap(t -> t.getTaskId(), t -> t));
        this.isIteration = nodeCycle.getVertexGroup().getCycleGroupMeta().isIterative();
        this.pipelineId = nodeCycle.getPipelineId();
        this.resultManager = context.getResultManager();

        inputExchangeMode = DataExchangeMode.BATCH;
        if (nodeCycle.isPipelineDataLoop()) {
            outputExchangeMode = DataExchangeMode.PIPELINE;
        } else {
            outputExchangeMode = DataExchangeMode.BATCH;
        }

        List<WorkerInfo> workers = this.context.assign(cycle);
        if (workers != null && workers.isEmpty()) {
            throw new GeaflowRuntimeException(String.format("failed to assign resource for cycle %s", null));
        }
        this.eventBuilder = new SchedulerEventBuilder(context, outputExchangeMode, resultManager);
        if (nodeCycle.getType() == ExecutionCycleType.ITERATION_WITH_AGG) {
            this.aggregator = new SchedulerGraphAggregateProcessor(nodeCycle,
                (AbstractCycleSchedulerContext) context, resultManager);
        }
    }

    @Override
    public void close() {
        super.close();
        iterationIdToFinishedTasks.clear();
        responseEventPool.clear();
        if (((AbstractCycleSchedulerContext) context).getParentContext() == null) {
            ShuffleDataManager.getInstance().release(pipelineId);
            ShuffleManager.getInstance().close();
        }
        if (((AbstractCycleSchedulerContext) context).getParentContext() == null) {
            context.close();
        }
        LOGGER.info("cycle {} closed", cycleLogTag);
    }

    @Override
    public void handleEvent(IEvent event) {
        LOGGER.info("{} handle event {}", cycleLogTag, event);
        switch (event.getEventType()) {
            case DONE:
                DoneEvent doneEvent = (DoneEvent) event;
                if (doneEvent.getSourceEvent() == EventType.LAUNCH_SOURCE) {
                    if (!taskIdToFinishedSourceIds.containsKey(doneEvent.getTaskId())) {
                        taskIdToFinishedSourceIds.put(doneEvent.getTaskId(), doneEvent.getWindowId());
                        if (taskIdToFinishedSourceIds.size() == nodeCycle.getCycleHeads().size()) {
                            long allSourceFinishIterationId =
                                taskIdToFinishedSourceIds.values().stream().max(Long::compareTo).get();
                            ((AbstractCycleSchedulerContext) context)
                                .setTerminateIterationId(allSourceFinishIterationId);
                            LOGGER.info("{} all source is finished at {}", cycleLogTag, allSourceFinishIterationId);

                            ICycleSchedulerContext parentContext = ((AbstractCycleSchedulerContext) context).getParentContext();
                            if (parentContext != null) {
                                DoneEvent cycleDoneEvent = new DoneEvent(parentContext.getCycle().getCycleId(),
                                    doneEvent.getWindowId(), doneEvent.getTaskId(),
                                    EventType.LAUNCH_SOURCE, doneEvent.getCycleId());
                                RpcClient.getInstance().processPipeline(cycle.getDriverId(), cycleDoneEvent);
                            }
                        }
                    }
                } else {
                    responseEventPool.notifyEvent(event);
                }
                break;
            case COMPOSE:
                for (IEvent e : ((ComposeEvent) event).getEventList()) {
                    handleEvent(e);
                }
                break;
            default:
                throw new RuntimeException(String.format("not supported event %s", event));
        }
    }

    protected void execute(long iterationId) {
        String iterationLogTag = getCycleIterationTag(iterationId);
        LOGGER.info("{} execute", iterationLogTag);
        iterationIdToFinishedTasks.put(iterationId, new ArrayList<>());
        CycleMetrics cycleMetrics = new CycleMetrics(LoggerFormatter.getCycleName(cycle.getCycleId(), iterationId),
            cycle.getPipelineName(),
            nodeCycle.getName());
        cycleMetrics.setStartTime(System.currentTimeMillis());
        iterationIdToMetrics.put(iterationId, cycleMetrics);
        List<ICycleSchedulerContext.SchedulerState> schedulerStates = context.getSchedulerState(iterationId);
        Map<Integer, IEvent> events;
        if (schedulerStates == null) {
            // Default execute trigger.
            events = eventBuilder.build(ICycleSchedulerContext.SchedulerState.EXECUTE, iterationId);
        } else {
            events = buildEvents(schedulerStates, iterationId);
        }
        for (Map.Entry<Integer, IEvent> entry : events.entrySet()) {
            ExecutionTask task = cycleTasks.get(entry.getKey());

            LOGGER.info("{} submit event {} on worker {} host {} process {}",
                getCycleTaskLogTag(iterationId, task.getIndex()),
                entry.getValue(),
                task.getWorkerInfo().getWorkerIndex(), task.getWorkerInfo().getHost(),
                task.getWorkerInfo().getProcessId());

            RpcClient.getInstance().processContainer(task.getWorkerInfo().getContainerName(), entry.getValue());
        }
    }

    private Map<Integer, IEvent> buildEvents(List<ICycleSchedulerContext.SchedulerState> states, long iterationId) {
        List<Map<Integer, IEvent>> list = new ArrayList<>();
        for (ICycleSchedulerContext.SchedulerState state : states) {
            list.add(eventBuilder.build(state, iterationId));
        }
        return mergeEvents(list);
    }

    protected void finish(long iterationId) {
        String iterationLogTag = getCycleIterationTag(iterationId);
        if (iterationIdToFinishedTasks.get(iterationId) == null) {
            // Unexpected to reach here.
            throw new GeaflowRuntimeException(String.format("fatal: %s result is unregistered",
                iterationLogTag));
        }

        int expectedResponseSize = nodeCycle.getCycleTails().size();
        while (iterationIdToFinishedTasks.get(iterationId).size() != expectedResponseSize) {
            IEvent response = responseEventPool.waitEvent();
            DoneEvent event = (DoneEvent) response;
            // Get iterationId from task.
            long currentTaskIterationId = event.getWindowId();
            if (!iterationIdToFinishedTasks.containsKey(currentTaskIterationId)) {
                throw new GeaflowRuntimeException(
                    String.format("%s finish error, current response iterationId %s, current waiting iterationIds %s",
                        cycleLogTag,
                        currentTaskIterationId,
                        iterationIdToFinishedTasks.keySet()));
            }
            iterationIdToFinishedTasks.get(currentTaskIterationId).add(response);
            LOGGER.info("{} receive event {}, iterationId {}, current response iterationId {}, "
                    + "received response {}/{}, duration {}ms",
                cycleLogTag, event,
                iterationId,
                currentTaskIterationId,
                iterationIdToFinishedTasks.get(currentTaskIterationId).size(),
                expectedResponseSize,
                System.currentTimeMillis() - iterationIdToMetrics.get(iterationId).getStartTime());
        }

        // Get current iteration result.
        List<IEvent> responses = iterationIdToFinishedTasks.remove(iterationId);
        for (IEvent e : responses) {
            registerResults((DoneEvent) e);
        }

        CycleMetrics cycleMetrics = iterationIdToMetrics.remove(iterationId);
        collectEventMetrics(cycleMetrics, responses);
        StatsCollectorFactory.getInstance().getPipelineStatsCollector().reportCycleMetrics(cycleMetrics);
        LOGGER.info("{} finished iterationId {}, {}", iterationLogTag, iterationId, cycleMetrics);
    }

    protected List<IResult> finish() {
        String finishLogTag = getCycleFinishTag();
        CycleMetrics cycleMetrics = new CycleMetrics(getCycleFinishName(), cycle.getPipelineName(), nodeCycle.getName());
        cycleMetrics.setStartTime(System.currentTimeMillis());

        Map<Integer, IEvent> events = eventBuilder.build(ICycleSchedulerContext.SchedulerState.FINISH,
            context.getCurrentIterationId());
        for (Map.Entry<Integer, IEvent> entry : events.entrySet()) {
            ExecutionTask task = cycleTasks.get(entry.getKey());
            RpcClient.getInstance().processContainer(task.getWorkerInfo().getContainerName(), entry.getValue());
            LOGGER.info("{} submit finish event {} ", finishLogTag, entry);
        }

        // Need receive all tail responses.
        int responseCount = 0;

        List<IEvent> responses = new ArrayList<>();
        while (true) {
            IEvent e = responseEventPool.waitEvent();
            DoneEvent<List<IResult>> event = (DoneEvent) e;
            switch (event.getSourceEvent()) {
                case EXECUTE_COMPUTE:
                    responses.add(event);
                    break;
                default:
                    responseCount++;
                    break;
            }
            if (responseCount == cycleTasks.size()) {
                LOGGER.info("{} all task result collected", finishLogTag);
                break;
            }
        }
        if (!responses.isEmpty()) {
            for (IEvent e : responses) {
                registerResults((DoneEvent) e);
            }

            collectEventMetrics(cycleMetrics, responses);
            StatsCollectorFactory.getInstance().getPipelineStatsCollector().reportCycleMetrics(cycleMetrics);
            LOGGER.info("{} finished {}", finishLogTag, cycleMetrics);
        }

        return context.getResultManager().getDataResponse();
    }

    private void registerResults(DoneEvent<Map<Integer, IResult>> event) {

        if (event.getResult() != null) {
            // Register result to resultManager.
            for (IResult result : event.getResult().values()) {
                LOGGER.info("{} register result for {}", event, result.getId());
                if (result.getType() == CollectType.RESPONSE && result.getId() != COLLECT_DATA_EDGE_ID) {
                    LOGGER.info("do aggregate, result {}", result.getResponse());
                    aggregator.aggregate(result.getResponse());
                } else {
                    resultManager.register(result.getId(), result);
                }
            }
        }
    }

    private void collectEventMetrics(CycleMetrics cycleMetrics, List<IEvent> responses) {

        final long duration = System.currentTimeMillis() - cycleMetrics.getStartTime();
        final int totalTaskNum = responses.size();
        long totalExecuteTime = 0;
        long totalGcTime = 0;
        long slowestTaskExecuteTime = 0;
        int slowestTaskId = 0;
        long totalInputRecords = 0;
        long totalInputBytes = 0;
        long totalOutputRecords = 0;
        long totalOutputBytes = 0;

        for (IEvent response : responses) {
            DoneEvent event = (DoneEvent) response;
            if (event.getEventMetrics() != null) {
                totalExecuteTime += event.getEventMetrics().getExecuteTime();
                totalExecuteTime += event.getEventMetrics().getGcTime();
                totalGcTime += event.getEventMetrics().getGcTime();
                if (event.getEventMetrics().getExecuteTime() > slowestTaskExecuteTime) {
                    slowestTaskExecuteTime = event.getEventMetrics().getExecuteTime();
                    slowestTaskId = event.getTaskId();
                }
                totalInputRecords += event.getEventMetrics().getInputRecords();
                totalInputBytes += event.getEventMetrics().getInputBytes();
                totalOutputRecords += event.getEventMetrics().getOutputRecords();
                totalOutputBytes += event.getEventMetrics().getOutputBytes();
            }
        }
        cycleMetrics.setTotalTasks(responses.size());
        cycleMetrics.setDuration(duration);
        cycleMetrics.setAvgGcTime(totalGcTime / totalTaskNum);
        cycleMetrics.setAvgExecuteTime(totalExecuteTime / totalTaskNum);
        cycleMetrics.setSlowestTaskExecuteTime(slowestTaskExecuteTime);
        cycleMetrics.setSlowestTask(slowestTaskId);
        cycleMetrics.setOutputRecords(totalOutputRecords);
        cycleMetrics.setOutputKb(totalOutputBytes / 1024);
    }

    private Map<Integer, IEvent> mergeEvents(List<Map<Integer, IEvent>> list) {
        Map<Integer, IEvent> result = new LinkedHashMap<>();
        for (ExecutionTask task : nodeCycle.getTasks()) {
            List<IEvent> events = new ArrayList<>();
            for (Map<Integer, IEvent> es : list) {
                if (es.containsKey(task.getTaskId())) {
                    IEvent e = es.get(task.getTaskId());
                    if (e.getEventType() == EventType.COMPOSE) {
                        events.addAll(((ComposeEvent) e).getEventList());
                    } else {
                        events.add(e);
                    }
                }
            }
            if (!events.isEmpty()) {
                result.put(task.getTaskId(), new ComposeEvent(task.getWorkerInfo().getWorkerIndex(), events));
            }
        }
        return result;
    }

    private String getCycleName(long iterationId) {
        return LoggerFormatter.getCycleTag(nodeCycle.getPipelineName(), nodeCycle.getCycleId(), iterationId);
    }

    private String getCycleTaskName(int vertexId, int taskIndex) {
        return LoggerFormatter.getTaskLog(nodeCycle.getPipelineName(), nodeCycle.getCycleId(), vertexId, taskIndex);
    }

    private String getCycleTaskLogTag(long iterationId, int taskIndex) {
        if (isIteration) {
            return LoggerFormatter.getTaskLog(nodeCycle.getPipelineName(),
                nodeCycle.getCycleId(), iterationId, taskIndex);
        } else {
            return LoggerFormatter.getTaskLog(nodeCycle.getPipelineName(),
                nodeCycle.getCycleId(), taskIndex);
        }
    }

    private String getCycleIterationTag(long iterationId) {
        if (!isIteration) {
            return cycleLogTag;
        } else {
            return LoggerFormatter.getCycleTag(cycle.getPipelineName(), cycle.getCycleId(), iterationId);

        }
    }

    private String getCycleFinishTag() {
        return LoggerFormatter.getCycleTag(cycle.getPipelineName(), cycle.getCycleId(), FINISH_TAG);
    }

    private String getCycleFinishName() {
        return LoggerFormatter.getCycleName(cycle.getCycleId(), FINISH_TAG);
    }
}
