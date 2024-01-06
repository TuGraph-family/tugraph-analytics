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

package com.antgroup.geaflow.runtime.core.worker;

import com.antgroup.geaflow.api.trait.CancellableTrait;
import com.antgroup.geaflow.cluster.collector.AbstractPipelineOutputCollector;
import com.antgroup.geaflow.cluster.protocol.EventType;
import com.antgroup.geaflow.cluster.protocol.InputMessage;
import com.antgroup.geaflow.cluster.response.IResult;
import com.antgroup.geaflow.cluster.rpc.RpcClient;
import com.antgroup.geaflow.cluster.worker.IWorker;
import com.antgroup.geaflow.cluster.worker.IWorkerContext;
import com.antgroup.geaflow.collector.ICollector;
import com.antgroup.geaflow.collector.IResultCollector;
import com.antgroup.geaflow.common.exception.GeaflowRuntimeException;
import com.antgroup.geaflow.common.metric.EventMetrics;
import com.antgroup.geaflow.core.graph.util.ExecutionTaskUtils;
import com.antgroup.geaflow.model.record.BatchRecord;
import com.antgroup.geaflow.model.record.RecordArgs;
import com.antgroup.geaflow.runtime.core.protocol.DoneEvent;
import com.antgroup.geaflow.runtime.core.worker.context.AbstractWorkerContext;
import com.antgroup.geaflow.shuffle.message.PipelineMessage;
import com.antgroup.geaflow.shuffle.serialize.IMessageIterator;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractAlignedWorker<T, O> implements IWorker<BatchRecord<T>, O> {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractAlignedWorker.class);

    private static final int DEFAULT_TIMEOUT_MS = 100;

    protected AbstractWorkerContext context;
    protected InputReader<T> inputReader;
    protected Map<Long, Long> windowCount;
    protected Map<Long, List<PipelineMessage<T>>> windowMessageCache;
    protected volatile boolean running;

    public AbstractAlignedWorker() {
        this.inputReader = new InputReader<>();
        this.windowCount = new HashMap<>();
        this.windowMessageCache = new HashMap<>();
        this.running = false;
    }

    @Override
    public void open(IWorkerContext workerContext) {
        this.context = (AbstractWorkerContext) workerContext;
        this.running = true;
    }

    @Override
    public IWorkerContext getWorkerContext() {
        return context;
    }

    public InputReader<T> getInputReader() {
        return inputReader;
    }

    /**
     * Fetch message from input queue and trigger aligned compute all the time,
     * and finish until total batch count has fetched.
     */
    public void alignedProcess(long totalWindowCount) {
        long processedWindowCount = 0;
        long fetchCost = 0;
        while (processedWindowCount < totalWindowCount && running) {
            try {
                long fetchStart = System.currentTimeMillis();
                InputMessage<T> input = this.inputReader.poll(DEFAULT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
                fetchCost += System.currentTimeMillis() - fetchStart;
                if (input != null) {
                    long windowId = input.getWindowId();
                    if (input.getMessage() != null) {
                        PipelineMessage<T> message = input.getMessage();
                        processMessage(windowId, message);
                    } else {
                        this.context.getEventMetrics().addShuffleReadCostMs(fetchCost);
                        fetchCost = 0;
                        long totalCount = input.getWindowCount();
                        processBarrier(windowId, totalCount);
                        processedWindowCount++;
                    }
                }
            } catch (Throwable t) {
                LOGGER.error(t.getMessage(), t);
                throw new GeaflowRuntimeException(t);
            }
        }
        if (!running) {
            LOGGER.info("{} worker terminated", context.getTaskId());
        }
    }

    /**
     * Tell scheduler finish and send back response to scheduler.
     */
    protected void finishWindow(long windowId) {
        Map<Integer, IResult<?>> results = new HashMap<>();
        List<ICollector<?>> collectors = this.context.getCollectors();
        if (ExecutionTaskUtils.isCycleTail(context.getExecutionTask())) {
            for (int i = 0; i < collectors.size(); i++) {
                IResultCollector<?> responseCollector = (IResultCollector<?>) collectors.get(i);
                IResult<?> result = (IResult<?>) responseCollector.collectResult();
                if (result != null) {
                    results.put(result.getId(), result);
                }
            }

            // Tell scheduler finish or response.
            EventMetrics eventMetrics = this.context.isIterativeTask() ? this.context.getEventMetrics() : null;
            DoneEvent<?> done = new DoneEvent<>(context.getCycleId(), windowId, context.getTaskId(),
                EventType.EXECUTE_COMPUTE, results, eventMetrics);
            RpcClient.getInstance().processPipeline(context.getDriverId(), done);
        }
    }


    protected void updateWindowId(long windowId) {
        context.setCurrentWindowId(windowId);
        for (ICollector<?> collector : this.context.getCollectors()) {
            if (collector instanceof AbstractPipelineOutputCollector) {
                ((AbstractPipelineOutputCollector<?>) collector).setWindowId(windowId);
            }
        }
    }

    /**
     * Trigger worker to process message.
     */
    private void processMessage(long windowId, PipelineMessage<T> message) {
        if (windowId > context.getCurrentWindowId()) {
            if (windowMessageCache.containsKey(windowId)) {
                windowMessageCache.get(windowId).add(message);
            } else {
                List<PipelineMessage<T>> cache = new ArrayList<>();
                cache.add(message);
                windowMessageCache.put(windowId, cache);
            }
        } else {
            processMessageEvent(windowId, message);
        }
    }

    /**
     * Trigger worker to process buffered message.
     */
    private void processBarrier(long windowId, long totalCount) {
        processBufferedMessages(windowId);

        long processCount = 0;
        if (windowCount.containsKey(windowId)) {
            processCount = windowCount.remove(windowId);
        }

        if (totalCount != processCount) {
            LOGGER.error("taskId {} {} mismatch, TotalCount:{} != ProcessCount:{}",
                context.getTaskId(), totalCount, totalCount, processCount);
            throw new GeaflowRuntimeException(String.format("taskId %s mismatch, TotalCount:%s != ProcessCount:%s",
                context.getTaskId(), totalCount, processCount));
        }
        context.getEventMetrics().addShuffleReadRecords(totalCount);

        long currentWindowId = context.getCurrentWindowId();
        finish(currentWindowId);
        updateWindowId(currentWindowId + 1);
    }

    /**
     * Process message event and trigger worker to process.
     */
    private void processMessageEvent(long windowId, PipelineMessage<T> message) {
        IMessageIterator<T> messageIterator = message.getMessageIterator();
        process(new BatchRecord<>(message.getRecordArgs(), messageIterator));

        long count = messageIterator.getSize();
        messageIterator.close();

        // Aggregate message not take into account when check message count.
        if (!message.getRecordArgs().getName().equals(RecordArgs.GraphRecordNames.Aggregate.name())) {
            if (!windowCount.containsKey(windowId)) {
                windowCount.put(windowId, count);
            } else {
                long oldCounter = windowCount.get(windowId);
                windowCount.put(windowId, oldCounter + count);
            }
        }
    }

    /**
     * Process buffered messages.
     */
    private void processBufferedMessages(long windowId) {
        if (windowMessageCache.containsKey(windowId)) {
            List<PipelineMessage<T>> cacheMessages = windowMessageCache.get(windowId);
            for (PipelineMessage<T> message : cacheMessages) {
                processMessageEvent(windowId, message);
            }
            windowMessageCache.remove(windowId);
        }
    }

    @Override
    public void interrupt() {
        this.running = false;
        if (context.getProcessor() instanceof CancellableTrait) {
            ((CancellableTrait) context.getProcessor()).cancel();
        }
    }

    @Override
    public void close() {
        if (context != null) {
            context.close();
        }
        windowCount.clear();
        windowMessageCache.clear();
    }
}
