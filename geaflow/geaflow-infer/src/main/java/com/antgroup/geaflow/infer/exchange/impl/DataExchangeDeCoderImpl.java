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

package com.antgroup.geaflow.infer.exchange.impl;

import com.antgroup.geaflow.common.exception.GeaflowRuntimeException;
import com.antgroup.geaflow.infer.exchange.IDecoder;
import com.antgroup.geaflow.infer.exchange.serialize.Unpickler;
import java.io.IOException;

public class DataExchangeDeCoderImpl<T> implements IDecoder<T> {

    private final transient Unpickler unpickler;

    public DataExchangeDeCoderImpl() {
        this.unpickler = new Unpickler();
    }

    @Override
    public T decode(byte[] bytes) {
        try {
            Object res = unpickler.loads(bytes);
            return (T) res;
        } catch (Exception e) {
            throw new GeaflowRuntimeException("unpick object error", e);
        }
    }

    @Override
    public void close() throws IOException {
        unpickler.close();
    }
}
