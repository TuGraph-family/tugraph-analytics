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

package com.antgroup.geaflow.common.type.primitive;

import com.antgroup.geaflow.common.type.IType;
import com.antgroup.geaflow.common.type.Types;
import com.google.common.primitives.Longs;

public class LongType implements IType<Long> {

    public static final LongType INSTANCE = new LongType();

    @Override
    public String getName() {
        return Types.TYPE_NAME_LONG;
    }

    @Override
    public Class<Long> getTypeClass() {
        return Long.class;
    }

    @Override
    public byte[] serialize(Long obj) {
        return Longs.toByteArray(obj);
    }

    @Override
    public Long deserialize(byte[] bytes) {
        return Longs.fromByteArray(bytes);
    }

    @Override
    public int compare(Long a, Long b) {
        return Types.compare(a, b);
    }

    @Override
    public boolean isPrimitive() {
        return true;
    }

    @Override
    public String toString() {
        return getName();
    }
}
