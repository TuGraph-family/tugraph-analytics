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
import com.antgroup.geaflow.common.metric.EventMetrics;
import com.antgroup.geaflow.common.shuffle.DataExchangeMode;
import com.antgroup.geaflow.common.utils.FutureUtil;
import com.antgroup.geaflow.common.utils.LoggerFormatter;
import com.antgroup.geaflow.core.graph.ExecutionTask;
import com.antgroup.geaflow.core.graph.ExecutionVertex;
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
import java.util.Objects;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is pipeline cycle scheduler impl.
 */
public class PipelineCycleScheduler<E>
    extends AbstractCycleScheduler<List<IResult>, E> implements IEventListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(PipelineCycleScheduler.class);

    private ExecutionNodeCycle nodeCycle;
    private CycleResultManager resultManager;

    private boolean enableSchedulerDebug = false;
    private CycleResponseEventPool<IEvent> responseEventPool;
    private HashMap<Long, List<IEvent>> iterationIdToFinishedTasks;
    private Map<Integer, EventMetrics[]> vertexIdToMetrics;
    private Map<Integer, ExecutionTask> cycleTasks;
    private Map<Integer, Long> taskIdToFinishedSourceIds;
    private String pipelineName;
    private long pipelineId;
    private int cycleId;
    private long scheduleStartTime;
    private boolean isIteration;

    private DataExchangeMode inputExchangeMode;
    private DataExchangeMode outputExchangeMode;

    private SchedulerEventBuilder eventBuilder;
    private SchedulerGraphAggregateProcessor aggregator;
    private String cycleLogTag;

    public PipelineCycleScheduler() {
    }

    @Override
    public void init(ICycleSchedulerContext context) {
        super.init(context);
        this.responseEventPool = new CycleResponseEventPool<>();
        this.iterationIdToFinishedTasks = new HashMap<>();
        this.taskIdToFinishedSourceIds = new HashMap<>();
        this.nodeCycle = (ExecutionNodeCycle) context.getCycle();
        this.cycleTasks = nodeCycle.getTasks().stream().collect(Collectors.toMap(t -> t.getTaskId(), t -> t));
        this.isIteration = nodeCycle.getVertexGroup().getCycleGroupMeta().isIterative();
        this.pipelineName = nodeCycle.getPipelineName();
        this.pipelineId = nodeCycle.getPipelineId();
        this.cycleId = nodeCycle.getCycleId();
        this.resultManager = context.getResultManager();
        this.cycleLogTag = LoggerFormatter.getCycleTag(this.pipelineName, this.cycleId);
        this.initMetrics();

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

    private void initMetrics() {
        this.scheduleStartTime = System.currentTimeMillis();
        this.vertexIdToMetrics = new HashMap<>();
        Map<Integer, ExecutionVertex> vertexMap = this.nodeCycle.getVertexGroup().getVertexMap();
        for (Map.Entry<Integer, ExecutionVertex> entry : vertexMap.entrySet()) {
            Integer vertexId = entry.getKey();
            ExecutionVertex vertex = entry.getValue();
            this.vertexIdToMetrics.put(vertexId, new EventMetrics[vertex.getParallelism()]);
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
        LOGGER.info("{} closed", cycleLogTag);
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
                                DoneEvent<?> cycleDoneEvent = new DoneEvent<>(parentContext.getCycle().getCycleId(),
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
        List<ICycleSchedulerContext.SchedulerState> schedulerStates = context.getSchedulerState(iterationId);
        Map<Integer, IEvent> events;
        if (schedulerStates == null) {
            // Default execute trigger.
            events = eventBuilder.build(ICycleSchedulerContext.SchedulerState.EXECUTE, iterationId);
        } else {
            events = buildEvents(schedulerStates, iterationId);
        }
        int eventSize = events.size();
        List<Future<IEvent>> submitFutures = new ArrayList<>(eventSize);
        for (Map.Entry<Integer, IEvent> entry : events.entrySet()) {
            ExecutionTask task = cycleTasks.get(entry.getKey());

            String taskTag = this.isIteration
                ? LoggerFormatter.getTaskTag(this.pipelineName, this.cycleId, iterationId,
                task.getTaskId(), task.getVertexId(), task.getIndex(), task.getParallelism())
                : LoggerFormatter.getTaskTag(this.pipelineName, this.cycleId, task.getTaskId(),
                task.getVertexId(), task.getIndex(), task.getParallelism());
            LOGGER.info("{} submit event {} on worker {} host {} process {}",
                taskTag,
                entry.getValue(),
                task.getWorkerInfo().getWorkerIndex(),
                task.getWorkerInfo().getHost(),
                task.getWorkerInfo().getProcessId());

            Future<IEvent> future = RpcClient.getInstance()
                .processContainer(task.getWorkerInfo().getContainerName(), entry.getValue());
            submitFutures.add(future);
        }

        FutureUtil.wait(submitFutures);
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
        }

        // Get current iteration result.
        List<IEvent> responses = iterationIdToFinishedTasks.remove(iterationId);
        for (IEvent e : responses) {
            registerResults((DoneEvent) e);
        }

        if (this.isIteration) {
            this.collectEventMetrics(responses, iterationId);
        }
        LOGGER.info("{} finished iterationId {}", iterationLogTag, iterationId);
    }

    protected List<IResult> finish() {
        long finishIterationId = this.isIteration
            ? this.context.getFinishIterationId() + 1 : this.context.getFinishIterationId();
        String finishLogTag = this.getCycleIterationTag(finishIterationId);

        Map<Integer, IEvent> events = eventBuilder.build(ICycleSchedulerContext.SchedulerState.FINISH,
            context.getCurrentIterationId());
        List<Future<IEvent>> submitFutures = new ArrayList<>(events.size());
        for (Map.Entry<Integer, IEvent> entry : events.entrySet()) {
            ExecutionTask task = cycleTasks.get(entry.getKey());
            Future<IEvent> future = RpcClient.getInstance()
                .processContainer(task.getWorkerInfo().getContainerName(), entry.getValue());
            LOGGER.info("{} submit finish event {} ", finishLogTag, entry);
            submitFutures.add(future);
        }
        FutureUtil.wait(submitFutures);

        // Need receive all tail responses.
        int responseCount = 0;

        List<IEvent> resultResponses = new ArrayList<>(this.cycleTasks.size());
        List<IEvent> metricResponses = new ArrayList<>(this.cycleTasks.size());
        while (true) {
            IEvent e = responseEventPool.waitEvent();
            DoneEvent<List<IResult>> event = (DoneEvent) e;
            switch (event.getSourceEvent()) {
                case EXECUTE_COMPUTE:
                    resultResponses.add(event);
                    break;
                default:
                    metricResponses.add(event);
                    responseCount++;
                    break;
            }
            if (responseCount == cycleTasks.size()) {
                LOGGER.info("{} all task result collected", finishLogTag);
                break;
            }
        }
        if (!resultResponses.isEmpty()) {
            for (IEvent e : resultResponses) {
                registerResults((DoneEvent) e);
            }
        }
        if (!metricResponses.isEmpty()) {
            this.collectEventMetrics(metricResponses, finishIterationId);
            LOGGER.info("{} finished", finishLogTag);
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

    private void collectEventMetrics(List<IEvent> responses, long windowId) {
        Map<Integer, List<EventMetrics>> vertexId2metrics = responses.stream()
            .map(e -> ((DoneEvent<?>) e).getEventMetrics())
            .filter(Objects::nonNull)
            .collect(Collectors.groupingBy(EventMetrics::getVertexId));

        long duration = System.currentTimeMillis() - this.scheduleStartTime;
        for (Map.Entry<Integer, List<EventMetrics>> entry : vertexId2metrics.entrySet()) {
            Integer vertexId = entry.getKey();
            List<EventMetrics> metrics = entry.getValue();
            EventMetrics[] previousMetrics = this.vertexIdToMetrics.get(vertexId);

            int taskNum = previousMetrics.length;
            int slowestTask = 0;
            long executeCostMs = 0;
            long totalExecuteTime = 0;
            long totalGcTime = 0;
            long slowestTaskExecuteTime = 0;
            long totalInputRecords = 0;
            long totalInputBytes = 0;
            long totalOutputRecords = 0;
            long totalOutputBytes = 0;

            for (EventMetrics eventMetrics : metrics) {
                int index = eventMetrics.getIndex();
                EventMetrics previous = previousMetrics[index];
                if (previous == null) {
                    executeCostMs = eventMetrics.getProcessCostMs();
                    totalExecuteTime += executeCostMs;
                    totalGcTime += eventMetrics.getGcCostMs();
                    totalInputRecords += eventMetrics.getShuffleReadRecords();
                    totalInputBytes += eventMetrics.getShuffleReadBytes();
                    totalOutputRecords += eventMetrics.getShuffleWriteRecords();
                    totalOutputBytes += eventMetrics.getShuffleWriteBytes();
                } else {
                    executeCostMs = eventMetrics.getProcessCostMs() - previous.getProcessCostMs();
                    totalExecuteTime += executeCostMs;
                    totalGcTime += eventMetrics.getGcCostMs() - previous.getGcCostMs();
                    totalInputRecords += eventMetrics.getShuffleReadRecords() - previous.getShuffleReadRecords();
                    totalInputBytes += eventMetrics.getShuffleReadBytes() - previous.getShuffleReadBytes();
                    totalOutputRecords += eventMetrics.getShuffleWriteRecords() - previous.getShuffleWriteRecords();
                    totalOutputBytes += eventMetrics.getShuffleWriteBytes() - previous.getShuffleWriteBytes();
                }
                if (executeCostMs > slowestTaskExecuteTime) {
                    slowestTaskExecuteTime = executeCostMs;
                    slowestTask = index;
                }
                if (this.isIteration) {
                    previousMetrics[index] = eventMetrics;
                }
            }

            String metricName = this.isIteration
                ? LoggerFormatter.getCycleMetricName(this.cycleId, windowId, vertexId)
                : LoggerFormatter.getCycleMetricName(this.cycleId, vertexId);
            String opName = this.nodeCycle.getVertexGroup().getVertexMap().get(vertexId).getName();
            CycleMetrics cycleMetrics = CycleMetrics.build(
                metricName,
                this.pipelineName,
                opName,
                taskNum,
                slowestTask,
                this.scheduleStartTime,
                duration,
                totalExecuteTime,
                totalGcTime,
                slowestTaskExecuteTime,
                totalInputRecords,
                totalInputBytes,
                totalOutputRecords,
                totalOutputBytes
            );
            LOGGER.info("collect metric {} {}", metricName, cycleMetrics);
            StatsCollectorFactory.getInstance().getPipelineStatsCollector().reportCycleMetrics(cycleMetrics);
        }

        this.scheduleStartTime = System.currentTimeMillis();
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

    private String getCycleIterationTag(long iterationId) {
        return this.isIteration
            ? LoggerFormatter.getCycleTag(this.pipelineName, this.cycleId, iterationId)
            : LoggerFormatter.getCycleTag(this.pipelineName, this.cycleId);
    }
}
