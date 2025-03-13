/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.antgroup.geaflow.common.type.primitive;

import com.antgroup.geaflow.common.type.IType;
import com.antgroup.geaflow.common.type.Types;
import com.google.common.primitives.Ints;

public class FloatType implements IType<Float> {

    public static final FloatType INSTANCE = new FloatType();

    @Override
    public String getName() {
        return Types.TYPE_NAME_FLOAT;
    }

    @Override
    public Class<Float> getTypeClass() {
        return Float.class;
    }

    @Override
    public byte[] serialize(Float obj) {
        return Ints.toByteArray(Float.floatToIntBits(obj));
    }

    @Override
    public Float deserialize(byte[] bytes) {
        return Float.intBitsToFloat(Ints.fromByteArray(bytes));
    }

    @Override
    public int compare(Float a, Float b) {
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
