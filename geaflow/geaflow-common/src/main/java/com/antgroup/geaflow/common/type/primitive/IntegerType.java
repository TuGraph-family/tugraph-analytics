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
import com.google.common.primitives.Ints;

public class IntegerType implements IType<Integer> {

    public static final IntegerType INSTANCE = new IntegerType();

    @Override
    public String getName() {
        return Types.TYPE_NAME_INTEGER;
    }

    @Override
    public Class<Integer> getTypeClass() {
        return Integer.class;
    }

    @Override
    public byte[] serialize(Integer obj) {
        return Ints.toByteArray(obj);
    }

    @Override
    public Integer deserialize(byte[] bytes) {
        return Ints.fromByteArray(bytes);
    }

    @Override
    public int compare(Integer a, Integer b) {
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
