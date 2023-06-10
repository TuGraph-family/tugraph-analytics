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

package com.antgroup.geaflow.runtime.core.worker;

import com.antgroup.geaflow.cluster.protocol.Message;
import com.antgroup.geaflow.common.exception.GeaflowRuntimeException;

import java.io.Serializable;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class InputReader implements Serializable {

    private LinkedBlockingQueue<Message> inputQueue;

    public InputReader() {
        this.inputQueue = new LinkedBlockingQueue<>();
    }

    /**
     * Add message into input queue.
     */
    public void add(Message message) {
        inputQueue.add(message);
    }

    /**
     * Returns message from input queue.
     */
    public Message poll(long timeout, TimeUnit unit) {
        Message message;
        try {
            message = inputQueue.poll(timeout, unit);
        } catch (Throwable t) {
            throw new GeaflowRuntimeException(t);
        }
        return message;
    }
}
