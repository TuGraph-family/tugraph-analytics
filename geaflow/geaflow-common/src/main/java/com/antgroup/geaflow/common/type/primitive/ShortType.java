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
import com.google.common.primitives.Shorts;

public class ShortType implements IType<Short> {

    public static final ShortType INSTANCE = new ShortType();

    @Override
    public String getName() {
        return Types.TYPE_NAME_SHORT;
    }

    @Override
    public Class<Short> getTypeClass() {
        return Short.class;
    }

    @Override
    public byte[] serialize(Short obj) {
        return Shorts.toByteArray(obj);
    }

    @Override
    public Short deserialize(byte[] bytes) {
        return Shorts.fromByteArray(bytes);
    }

    @Override
    public int compare(Short a, Short b) {
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
