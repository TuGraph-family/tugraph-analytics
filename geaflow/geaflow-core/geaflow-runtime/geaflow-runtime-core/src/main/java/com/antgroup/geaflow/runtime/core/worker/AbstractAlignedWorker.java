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
import com.antgroup.geaflow.cluster.protocol.Message;
import com.antgroup.geaflow.cluster.response.IResult;
import com.antgroup.geaflow.cluster.response.ShardResult;
import com.antgroup.geaflow.cluster.worker.IWorker;
import com.antgroup.geaflow.cluster.worker.IWorkerContext;
import com.antgroup.geaflow.collector.ICollector;
import com.antgroup.geaflow.collector.IResultCollector;
import com.antgroup.geaflow.common.exception.GeaflowRuntimeException;
import com.antgroup.geaflow.common.utils.GcUtil;
import com.antgroup.geaflow.core.graph.util.ExecutionTaskUtils;
import com.antgroup.geaflow.io.CollectType;
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

public abstract class AbstractAlignedWorker<O> implements IWorker<BatchRecord, O> {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractAlignedWorker.class);

    protected AbstractWorkerContext context;
    protected InputReader inputReader;
    protected OutputWriter outputWriter;
    protected Map<Long, Long> windowCount;
    protected Map<Long, List<PipelineMessage>> windowMessageCache;
    protected volatile boolean running;

    public AbstractAlignedWorker() {
        this.inputReader = new InputReader();
        this.outputWriter = new OutputWriter();
        this.windowCount = new HashMap<>();
        this.windowMessageCache = new HashMap();
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

    public InputReader getInputReader() {
        return inputReader;
    }

    public OutputWriter getOutputWriter() {
        return outputWriter;
    }

    /**
     * Fetch message from input queue and trigger aligned compute all the time,
     * and finish until total batch count has fetched.
     */
    public void alignedProcess(long totalWindowCount) {
        long processedWindowCount = 0;
        while (processedWindowCount < totalWindowCount && running) {
            try {
                Message input = inputReader.poll(100, TimeUnit.MILLISECONDS);
                if (input != null) {
                    long windowId = input.getWindowId();
                    if (input.getMessage() != null) {
                        PipelineMessage message = input.getMessage();
                        processMessage(windowId, message);
                    } else {
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
        Map<Integer, IResult> results = new HashMap<>();
        List<ICollector> collectors = outputWriter.getCollectors();
        if (ExecutionTaskUtils.isCycleTail(context.getExecutionTask())) {
            long outputRecordNum = 0;
            long outputBytes = 0;
            for (int i = 0; i < collectors.size(); i++) {
                IResultCollector responseCollector = (IResultCollector) collectors.get(i);
                IResult result = (IResult) responseCollector.collectResult();
                if (result != null) {
                    results.put(result.getId(), result);
                    if (result.getType() == CollectType.FORWARD || result.getType() == CollectType.LOOP) {
                        outputRecordNum += ((ShardResult) result).getRecordNum();
                        outputBytes += ((ShardResult) result).getRecordBytes();
                    }
                }
            }
            context.getEventMetrics().setOutputRecords(outputRecordNum);
            context.getEventMetrics().setOutputBytes(outputBytes);

            // Tell scheduler finish or response.
            DoneEvent done = new DoneEvent(context.getCycleId(), windowId, context.getTaskId(),
                EventType.EXECUTE_COMPUTE, results, context.getEventMetrics());
            context.getPipelineMaster().send(done);
        }
    }


    protected void updateWindowId(long windowId) {
        context.getEventMetrics().setStartTs(System.currentTimeMillis());
        context.getEventMetrics().setStartGcTs(GcUtil.computeCurrentTotalGcTime());
        context.setCurrentWindowId(windowId);
        for (ICollector collector : outputWriter.getCollectors()) {
            if (collector instanceof AbstractPipelineOutputCollector) {
                ((AbstractPipelineOutputCollector) collector).setWindowId(windowId);
            }
        }
    }

    /**
     * Trigger worker to process message.
     */
    private void processMessage(long windowId, PipelineMessage message) {
        if (windowId > context.getCurrentWindowId()) {
            if (windowMessageCache.containsKey(windowId)) {
                windowMessageCache.get(windowId).add(message);
            } else {
                List cache = new ArrayList();
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
    private void processBarrier( long windowId, long totalCount) {
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
        context.getEventMetrics().setInputRecords(totalCount);

        long currentWindowId = context.getCurrentWindowId();
        finish(currentWindowId);
        updateWindowId(currentWindowId + 1);
    }

    /**
     * Process message event and trigger worker to process.
     */
    private void processMessageEvent(long windowId, PipelineMessage message) {
        IMessageIterator messageIterator = message.getMessageIterator();
        process(new BatchRecord<>(message.getRecordArgs(), messageIterator));

        long count = messageIterator.getSize();
        messageIterator.close();

        // Aggregate message not take into account when check message count.
        if (message.getRecordArgs().getName() != RecordArgs.GraphRecordNames.Aggregate.name()) {
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
            List<PipelineMessage> cacheMessages = windowMessageCache.get(windowId);
            for (PipelineMessage message : cacheMessages) {
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
