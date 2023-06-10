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

package com.antgroup.geaflow.collector.chain;

import com.antgroup.geaflow.collector.ICollector;
import com.antgroup.geaflow.common.exception.GeaflowRuntimeException;

public interface IChainCollector<T> extends ICollector<T> {

    /**
     * Process record value.
     */
    void process(T value);

    /**
     * Partition value.
     */
    default void partition(T value) {
        process(value);
    }

    @Override
    default void broadcast(T value) {
        throw new GeaflowRuntimeException("chain collector not support broadcast");
    }

    @Override
    default <KEY> void partition(KEY key, T record) {
        throw new GeaflowRuntimeException("chain collector not support key partition");
    }
}
