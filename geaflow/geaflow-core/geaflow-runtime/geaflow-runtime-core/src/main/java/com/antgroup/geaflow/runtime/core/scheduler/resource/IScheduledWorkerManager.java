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

package com.antgroup.geaflow.runtime.core.scheduler.resource;

import com.antgroup.geaflow.cluster.resourcemanager.WorkerInfo;
import java.util.List;
import java.util.Set;

public interface IScheduledWorkerManager<G, V> {

    /**
     * Init the worker manager by input graph.
     * The graph info will help to decide the total worker resources required.
     */
    void init(G graph);

    /**
     * Assign workers for execution task of input vertex.
     * @return Workers if assign worker succeed, otherwise empty.
     */
    List<WorkerInfo> assign(V vertex);

    /**
     * Release all worker resource for the input vertex.
     */
    void release(V vertex);

    /**
     * Clean worker runtime context for used workers by specified clean function.
     */
    void clean(CleanWorkerFunction cleaFunc);

    /**
     * Release all worker to master resource manager.
     */
    void close();

    /**
     * Function interface to clean runtime context for already assigned workers.
     */
    interface CleanWorkerFunction {

        void clean(Set<WorkerInfo> assignedWorkers);

    }
}
