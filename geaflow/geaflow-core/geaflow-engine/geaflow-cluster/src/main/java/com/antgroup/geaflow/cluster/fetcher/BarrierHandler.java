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

package com.antgroup.geaflow.cluster.fetcher;

import com.antgroup.geaflow.common.exception.GeaflowRuntimeException;
import com.antgroup.geaflow.shuffle.message.PipelineBarrier;
import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Process the barrier event.
 */
public class BarrierHandler implements Serializable {

    private static final Logger LOGGER = LoggerFactory.getLogger(BarrierHandler.class);

    private final int inputSliceNum;
    private final Map<Long, Set<PipelineBarrier>> barrierCache;

    private long finishedWindowId;
    private long totalWindowCount;
    private int taskId;

    public BarrierHandler(int taskId, int sliceNum) {
        this.inputSliceNum = sliceNum;
        this.barrierCache = new HashMap<>();
        this.finishedWindowId = -1;
        this.totalWindowCount = 0;
        this.taskId = taskId;
    }

    public boolean checkCompleted(PipelineBarrier barrier) {
        if (barrier.getWindowId() <= finishedWindowId) {
            throw new GeaflowRuntimeException(String.format("illegal state: taskId %s window %s has "
                + "finished, last finished window is: %s", taskId, barrier.getWindowId(), finishedWindowId));
        }
        long windowId = barrier.getWindowId();
        Set<PipelineBarrier> inputBarriers = barrierCache.computeIfAbsent(windowId,
            key -> new HashSet<>());
        inputBarriers.add(barrier);

        int barrierSize = inputBarriers.size();
        if (barrierSize == inputSliceNum) {
            inputBarriers = barrierCache.remove(windowId);
            finishedWindowId = windowId;
            totalWindowCount = inputBarriers.stream().mapToLong(PipelineBarrier::getCount).sum();
            inputBarriers.clear();
            return true;
        }
        return false;
    }

    public long getTotalWindowCount() {
        return totalWindowCount;
    }
}
