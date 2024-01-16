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

package com.antgroup.geaflow.runtime.core.worker.impl;

import com.antgroup.geaflow.cluster.worker.WorkerType;
import com.antgroup.geaflow.runtime.core.worker.AbstractUnAlignedWorker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UnAlignedComputeWorker<T, R> extends AbstractUnAlignedWorker<T, R> {

    private static final Logger LOGGER = LoggerFactory.getLogger(UnAlignedComputeWorker.class);

    public UnAlignedComputeWorker() {
        super();
    }

    @Override
    public void init(long windowId) {
        // TODO Processing in dynamic / stream scene.
        if (processingWindowIdQueue.isEmpty() && windowId <= context.getWindowId()) {
            super.init(windowId);
        }
        processingWindowIdQueue.add(windowId);
    }

    @Override
    public void finish(long windowId) {
        LOGGER.info("taskId {} finishes windowId {}, currentBatchId {}, real currentBatchId {}",
            context.getTaskId(), windowId, windowId, context.getCurrentWindowId());
        context.getProcessor().finish(windowId);
        finishWindow(windowId);
    }

    @Override
    public WorkerType getWorkerType() {
        return WorkerType.unaligned_compute;
    }
}
