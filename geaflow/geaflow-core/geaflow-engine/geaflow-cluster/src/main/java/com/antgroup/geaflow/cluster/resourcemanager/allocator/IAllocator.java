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

package com.antgroup.geaflow.cluster.resourcemanager.allocator;

import java.util.Collection;
import java.util.List;

public interface IAllocator<G, W> {

    AllocateStrategy DEFAULT_ALLOCATE_STRATEGY = AllocateStrategy.ROUND_ROBIN;

    enum AllocateStrategy {
        /**
         * Round-robin.
         */
        ROUND_ROBIN
    }

    @FunctionalInterface
    interface WorkerGroupByFunction<G, W> {

        /**
         * Get the group the worker belongs.
         */
        Comparable<G> getGroup(W worker);
    }

    /**
     * Strategy of this allocator.
     * @return allocate strategy
     */
    AllocateStrategy getStrategy();

    /**
     * Worker group selector of this allocator.
     * @return worker group selector
     */
    WorkerGroupByFunction<G, W> getWorkerGroupByFunction();

    /**
     * Allocate workers.
     * @param idleWorkers workers to allocate
     * @param num number
     * @return allocated workers
     */
    List<W> allocate(Collection<W> idleWorkers, int num);

}
