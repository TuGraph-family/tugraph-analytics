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

import com.antgroup.geaflow.common.serialize.SerializerFactory;
import com.antgroup.geaflow.common.serialize.impl.KryoSerializer;
import com.antgroup.geaflow.shuffle.api.pipeline.buffer.OutBuffer;
import com.esotericsoftware.kryo.KryoException;
import com.esotericsoftware.kryo.io.Input;
import java.io.InputStream;
import java.util.Locale;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MessageIterator<T> extends AbstractMessageIterator<T> {
    private static final Logger LOGGER = LoggerFactory.getLogger(MessageIterator.class);

    private final KryoSerializer kryoSerializer;
    private final Input input;

    public MessageIterator(OutBuffer outBuffer) {
        super(outBuffer);
        this.kryoSerializer = ((KryoSerializer) SerializerFactory.getKryoSerializer());
        this.input = new Input(this.inputStream);
    }

    public MessageIterator(InputStream inputStream) {
        super(inputStream);
        this.kryoSerializer = ((KryoSerializer) SerializerFactory.getKryoSerializer());
        this.input = new Input(inputStream);
    }

    public boolean hasNext() {
        if (currentValue != null) {
            return true;
        }
        try {
            currentValue = (T) kryoSerializer.getThreadKryo().readClassAndObject(input);
            return true;
        } catch (KryoException e) {
            if (e.getMessage().toLowerCase(Locale.ROOT).contains("buffer underflow")) {
                currentValue = null;
                return false;
            }
            LOGGER.error("deserialize failed", e);
            throw e;
        }
    }

}
