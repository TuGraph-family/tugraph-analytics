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

package com.antgroup.geaflow.shuffle.serialize;

import com.antgroup.geaflow.shuffle.api.pipeline.buffer.OutBuffer;

public interface IRecordSerializer<T> {

    /**
     * Serialize data to out buffer.
     *
     * @param record data.
     * @param isRetract if data is retract.
     * @param outBuffer buffer.
     */
    void serialize(T record, boolean isRetract, OutBuffer.BufferBuilder outBuffer);

}
