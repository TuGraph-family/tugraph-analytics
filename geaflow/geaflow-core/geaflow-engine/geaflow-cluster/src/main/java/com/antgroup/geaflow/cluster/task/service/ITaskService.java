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

package com.antgroup.geaflow.cluster.task.service;

public interface ITaskService<TASK> {

    /**
     * Start task service.
     */
    void start();

    /**
     * Process event on workerId.
     */
    void process(int workerId, TASK task);

    /**
     * Interrupt running task worker.
     */
    void interrupt(int workerId);

    /**
     * Shutdown task service.
     */
    void shutdown();
}
