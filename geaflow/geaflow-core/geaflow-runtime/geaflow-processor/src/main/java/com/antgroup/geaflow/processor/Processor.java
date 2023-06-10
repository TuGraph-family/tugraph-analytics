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

package com.antgroup.geaflow.processor;

import com.antgroup.geaflow.api.context.RuntimeContext;
import com.antgroup.geaflow.collector.ICollector;

import java.io.Serializable;
import java.util.List;

public interface Processor<T, R> extends Serializable {

    /**
     * Returns the op id.
     */
    int getId();

    /**
     * Operator open.
     */
    void open(List<ICollector> collector, RuntimeContext runtimeContext);

    /**
     * Initialize operator by windowId.
     */
    void init(long windowId);

    /**
     * Operator process value t.
     */
    R process(T t);

    /**
     * Finish processing of windowId.
     */
    void finish(long windowId);

    /**
     * Operator close.
     */
    void close();
}
