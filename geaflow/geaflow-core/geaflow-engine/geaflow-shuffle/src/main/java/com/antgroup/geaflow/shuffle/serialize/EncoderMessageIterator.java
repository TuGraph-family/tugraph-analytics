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
import java.io.InputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EncoderMessageIterator<T> extends AbstractMessageIterator<T> {

    private static final Logger LOGGER = LoggerFactory.getLogger(EncoderMessageIterator.class);

    private final IEncoder<T> encoder;

    public EncoderMessageIterator(OutBuffer outBuffer, IEncoder<T> encoder) {
        super(outBuffer);
        this.encoder = encoder;
    }

    public EncoderMessageIterator(InputStream inputStream, IEncoder<T> encoder) {
        super(inputStream);
        this.encoder = encoder;
    }

    @Override
    public boolean hasNext() {
        if (currentValue != null) {
            return true;
        }
        try {
            if (this.inputStream.available() > 0) {
                this.currentValue = this.encoder.decode(this.inputStream);
                return true;
            } else {
                return false;
            }
        } catch (IOException e) {
            LOGGER.error("encoder deserialize err", e);
            throw new GeaflowRuntimeException(RuntimeErrors.INST.shuffleDeserializeError(e.getMessage()), e);
        }
    }

}
