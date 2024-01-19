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

package com.antgroup.geaflow.cluster.worker;

import java.io.Serializable;

public interface IWorker<I, O> extends Serializable {

    /**
     * Open worker processor.
     */
    void open(IWorkerContext workerContext);

    /**
     * Init worker processor runtime info.
     */
    void init(long windowId);

    /**
     * Worker do processing of processor.
     */
    O process(I input);

    /**
     * Worker finish processing of processor.
     */
    void finish(long windowId);

    /**
     * Interrupt the processing of current window.
     */
    void interrupt();

    /**
     * Release worker resource if needed.
     */
    void close();

    /**
     * Returns the runtime context of worker.
     */
    IWorkerContext getWorkerContext();

    /**
     * Returns the worker type.
     */
    WorkerType getWorkerType();
}
