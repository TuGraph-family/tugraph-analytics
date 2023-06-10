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

import com.antgroup.geaflow.common.encoder.IEncoder;
import com.antgroup.geaflow.common.errorcode.RuntimeErrors;
import com.antgroup.geaflow.common.exception.GeaflowRuntimeException;
import com.antgroup.geaflow.shuffle.api.pipeline.buffer.OutBuffer;
import java.io.IOException;

public class EncoderRecordSerializer<T> implements IRecordSerializer<T> {

    private final IEncoder<T> encoder;

    public EncoderRecordSerializer(IEncoder<T> encoder) {
        this.encoder = encoder;
    }

    @Override
    public void serialize(T value, boolean isRetract, OutBuffer.BufferBuilder outBuffer) {
        try {
            this.encoder.encode(value, outBuffer.getOutputStream());
        } catch (IOException e) {
            throw new GeaflowRuntimeException(RuntimeErrors.INST.shuffleSerializeError(e.getMessage()), e);
        }
    }

}
