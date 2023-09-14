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

package com.antgroup.geaflow.runtime.core.scheduler.cycle;

public enum ExecutionCycleType {

    /**
     * A cycle that contains whole execution graph.
     */
    GRAPH,

    /**
     * A pipeline cycle fetch data and finally transfer output to downstream or sink op.
     */
    PIPELINE,

    /**
     * Iteration cycle that container data flow loop from cycle tail to cycle head.
     */
    ITERATION,

    /**
     * Iteration cycle that container data flow loop from cycle tail to cycle head with aggregation.
     */
    ITERATION_WITH_AGG,
}
