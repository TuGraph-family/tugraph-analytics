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
import com.antgroup.geaflow.common.exception.GeaflowRuntimeException;
import com.antgroup.geaflow.shuffle.message.PipelineMessage;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractAlignedWorker<T, O> extends AbstractComputeWorker<T, O> {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractAlignedWorker.class);

    protected Map<Long, List<PipelineMessage<T>>> windowMessageCache;

    public AbstractAlignedWorker() {
        super();
        this.windowMessageCache = new HashMap<>();
    }

    /**
     * Trigger worker to process message, need cache message in aligned worker.
     */
    @Override
    protected void processMessage(long windowId, PipelineMessage<T> message) {
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
    @Override
    protected void processBarrier(long windowId, long totalCount) {
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
        super.close();
        windowCount.clear();
        windowMessageCache.clear();
    }
}
