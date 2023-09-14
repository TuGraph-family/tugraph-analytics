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

package com.antgroup.geaflow.collector;

import com.antgroup.geaflow.api.collector.Collector;
import com.antgroup.geaflow.api.context.RuntimeContext;
import com.antgroup.geaflow.io.CollectType;

public interface ICollector<T> extends Collector<T> {

    /**
     * Returns op id.
     */
    int getId();

    /**
     * Returns tag.
     */
    String getTag();

    /**
     * Returns type.
     */
    CollectType getType();

    /**
     * Initialize collector.
     * @param runtimeContext The runtime context.
     */
    void setUp(RuntimeContext runtimeContext);

    /**
     * Broadcast value to downstream.
     */
    void broadcast(T value);

    /**
     * Partition value by key.
     */
    <KEY> void partition(KEY key, T value);

    /**
     * Finish flush.
     */
    void finish();

    /**
     * Close pipeline writer.
     */
    void close();

}
