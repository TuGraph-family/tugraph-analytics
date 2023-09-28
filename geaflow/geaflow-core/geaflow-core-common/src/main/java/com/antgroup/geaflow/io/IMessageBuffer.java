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

package com.antgroup.geaflow.io;

import java.util.concurrent.TimeUnit;

public interface IMessageBuffer<M> {

    /**
     * Push a message to this pipe.
     *
     * @param message message
     */
    void offer(M message);

    /**
     * Pull a record from this pipe with timeout.
     *
     * @param timeout timeout number
     * @param unit timeout unit
     * @return message
     */
    M poll(long timeout, TimeUnit unit);

}
