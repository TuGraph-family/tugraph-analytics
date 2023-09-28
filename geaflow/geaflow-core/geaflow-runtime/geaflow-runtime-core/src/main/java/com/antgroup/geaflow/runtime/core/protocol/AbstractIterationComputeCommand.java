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

package com.antgroup.geaflow.runtime.core.protocol;

import com.antgroup.geaflow.cluster.fetcher.ReFetchRequest;
import com.antgroup.geaflow.cluster.task.ITaskContext;
import com.antgroup.geaflow.runtime.core.worker.AbstractAlignedWorker;
import com.antgroup.geaflow.runtime.core.worker.context.AbstractWorkerContext;
import com.antgroup.geaflow.shuffle.message.PipelineMessage;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractIterationComputeCommand extends AbstractExecutableCommand {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractIterationComputeCommand.class);

    protected long fetchWindowId;
    protected long fetchCount;
    protected Map<Long, Long> windowCount;
    protected Map<Long, List<PipelineMessage>> batchMessageCache;

    public AbstractIterationComputeCommand(int workerId, int cycleId, long windowId, long fetchWindowId, long fetchCount) {
        super(workerId, cycleId, windowId);
        this.fetchWindowId = fetchWindowId;
        this.fetchCount = fetchCount;
        this.windowCount = new HashMap<>();
        this.batchMessageCache = new HashMap();
    }

    @Override
    public void execute(ITaskContext taskContext) {
        final long start = System.currentTimeMillis();
        super.execute(taskContext);
        AbstractAlignedWorker alignedWorker = (AbstractAlignedWorker) worker;
        alignedWorker.init(windowId);
        fetcherRunner.add(new ReFetchRequest(fetchWindowId, fetchCount));
        alignedWorker.alignedProcess(fetchCount);
        ((AbstractWorkerContext) this.context).getEventMetrics().addProcessCostMs(System.currentTimeMillis() - start);
    }

}
