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

package com.antgroup.geaflow.cluster.collector;

import com.antgroup.geaflow.cluster.protocol.OutputMessage;
import com.antgroup.geaflow.io.IMessageBuffer;

public interface IOutputMessageBuffer<T, R> extends IMessageBuffer<OutputMessage<T>> {

    /**
     * Emit a record.
     *
     * @param windowId window id
     * @param data data
     * @param isRetract if this data is retract
     * @param targetChannels target channels
     */
    void emit(long windowId, T data, boolean isRetract, int[] targetChannels);

    /**
     * For the consumer to finish.
     *
     * @param windowId window id
     * @param result finish result
     */
    void setResult(long windowId, R result);

    /**
     * For the producer to call finish.
     *
     * @param windowId window id
     * @return finish result
     */
    R finish(long windowId);

    /**
     * Meet error.
     *
     * @param t error
     */
    void error(Throwable t);

}
