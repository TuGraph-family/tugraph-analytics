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

package com.antgroup.geaflow.runtime.core.worker.context;

import com.antgroup.geaflow.cluster.worker.IWorkerContext;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WorkerContextManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(WorkerContextManager.class);

    private static ConcurrentHashMap<Integer, IWorkerContext> workerContexts = new ConcurrentHashMap<>();

    public static IWorkerContext get(int taskId) {
        return workerContexts.get(taskId);
    }

    public static void register(int taskId, WorkerContext workerContext) {
        LOGGER.info("taskId {} register worker context", taskId);
        workerContexts.put(taskId, workerContext);
    }

    public static synchronized void clear() {
        for (IWorkerContext workerContext : workerContexts.values()) {
            workerContext.close();
        }
        LOGGER.info("clear all worker context");
        workerContexts.clear();
    }
}
