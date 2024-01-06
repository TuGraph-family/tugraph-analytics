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

import com.antgroup.geaflow.cluster.task.ITaskContext;
import com.antgroup.geaflow.collector.ICollector;

public class WorkerContext extends AbstractWorkerContext {

    public WorkerContext(ITaskContext taskContext) {
        super(taskContext);
    }

    /**
     * Release worker resource.
     */
    @Override
    public void close() {
        for (ICollector<?> collector : collectors) {
            collector.close();
        }
        processor.close();
    }
}
