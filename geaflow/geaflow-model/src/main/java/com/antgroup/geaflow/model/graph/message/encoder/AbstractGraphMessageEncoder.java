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

package com.antgroup.geaflow.model.graph.message.encoder;

import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.encoder.IEncoder;
import com.antgroup.geaflow.common.encoder.impl.AbstractEncoder;
import com.antgroup.geaflow.common.errorcode.RuntimeErrors;
import com.antgroup.geaflow.common.exception.GeaflowRuntimeException;
import com.antgroup.geaflow.model.graph.message.IGraphMessage;
import java.io.IOException;
import java.io.OutputStream;

public abstract class AbstractGraphMessageEncoder<K, M, GRAPHMESSAGE extends IGraphMessage<K, M>>
    extends AbstractEncoder<GRAPHMESSAGE> {

    protected final IEncoder<K> keyEncoder;
    protected final IEncoder<M> msgEncoder;

    public AbstractGraphMessageEncoder(IEncoder<K> keyEncoder, IEncoder<M> msgEncoder) {
        this.keyEncoder = keyEncoder;
        this.msgEncoder = msgEncoder;
    }

    @Override
    public void init(Configuration config) {
        this.keyEncoder.init(config);
        this.msgEncoder.init(config);
    }

    @Override
    public void encode(GRAPHMESSAGE data, OutputStream outputStream) throws IOException {
        if (data == null) {
            throw new GeaflowRuntimeException(
                RuntimeErrors.INST.shuffleSerializeError("graph message can not be null"));
        }
        this.doEncode(data, outputStream);
    }

    public abstract void doEncode(GRAPHMESSAGE data, OutputStream outputStream) throws IOException;

}
