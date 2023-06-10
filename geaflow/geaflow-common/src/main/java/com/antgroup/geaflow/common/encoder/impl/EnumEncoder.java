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
import com.antgroup.geaflow.common.errorcode.RuntimeErrors;
import com.antgroup.geaflow.common.exception.GeaflowRuntimeException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;

public class EnumEncoder<T> extends AbstractEncoder<T> {

    private final Class<T> enumClass;
    private T[] values;
    private Map<T, Integer> value2ordinal;

    public EnumEncoder(Class<T> enumClass) {
        if (!Enum.class.isAssignableFrom(enumClass)) {
            throw new GeaflowRuntimeException(RuntimeErrors.INST.typeSysError(
                enumClass.getCanonicalName() + "is not an enum"));
        }
        this.enumClass = enumClass;
    }

    @Override
    public void init(Configuration config) {
        this.value2ordinal = new HashMap<>();
        this.values = this.enumClass.getEnumConstants();
        int i = 0;
        for (T value : this.values) {
            this.value2ordinal.put(value, ++i);
        }
    }

    @Override
    public void encode(T data, OutputStream outputStream) throws IOException {
        if (data == null) {
            Encoders.INTEGER.encode(NULL, outputStream);
            return;
        }
        Encoders.INTEGER.encode(this.value2ordinal.get(data), outputStream);
    }

    @Override
    public T decode(InputStream inputStream) throws IOException {
        int flag = Encoders.INTEGER.decode(inputStream);
        if (flag == NULL) {
            return null;
        }
        return this.values[flag - 1];
    }

}
