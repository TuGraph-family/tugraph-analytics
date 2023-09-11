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

package com.antgroup.geaflow.common.encoder.impl;

import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.encoder.Encoders;
import com.antgroup.geaflow.common.encoder.IEncoder;
import com.antgroup.geaflow.common.tuple.Tuple;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class TupleEncoder<F0, F1> extends AbstractEncoder<Tuple<F0, F1>> {

    private final IEncoder<F0> encoder0;
    private final IEncoder<F1> encoder1;

    public TupleEncoder(IEncoder<F0> encoder0, IEncoder<F1> encoder1) {
        this.encoder0 = encoder0;
        this.encoder1 = encoder1;
    }

    @Override
    public void init(Configuration config) {
        this.encoder0.init(config);
        this.encoder1.init(config);
    }

    @Override
    public void encode(Tuple<F0, F1> data,
                       OutputStream outputStream) throws IOException {
        int flag = data == null ? NULL : NOT_NULL;
        Encoders.INTEGER.encode(flag, outputStream);
        if (flag == NULL) {
            return;
        }
        this.encoder0.encode(data.f0, outputStream);
        this.encoder1.encode(data.f1, outputStream);
    }

    @Override
    public Tuple<F0, F1> decode(InputStream inputStream) throws IOException {
        int flag = Encoders.INTEGER.decode(inputStream);
        if (flag == NULL) {
            return null;
        }
        F0 f0 = this.encoder0.decode(inputStream);
        F1 f1 = this.encoder1.decode(inputStream);
        return Tuple.of(f0, f1);
    }

}
