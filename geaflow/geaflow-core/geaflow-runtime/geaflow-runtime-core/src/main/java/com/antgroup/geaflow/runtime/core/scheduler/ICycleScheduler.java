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

import com.antgroup.geaflow.runtime.core.scheduler.context.ICycleSchedulerContext;
import com.antgroup.geaflow.runtime.core.scheduler.result.IExecutionResult;

public interface ICycleScheduler<R, E> {

    /**
     * Initialize cycle scheduler by input context.
     * May include assign resource and initialize worker, and set up cycle schedule env.
     */
    void init(ICycleSchedulerContext context);

    /**
     * Execution all cycles.
     */
    IExecutionResult<R, E> execute();

    /**
     * Close the initialized resources and workers.
     */
    void close();

}
