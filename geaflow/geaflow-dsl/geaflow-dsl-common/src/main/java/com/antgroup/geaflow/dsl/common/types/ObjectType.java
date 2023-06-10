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

package com.antgroup.geaflow.dsl.common.types;

import com.antgroup.geaflow.common.serialize.SerializerFactory;
import com.antgroup.geaflow.common.type.IType;
import com.antgroup.geaflow.common.type.Types;

public class ObjectType implements IType<Object> {

    public static ObjectType INSTANCE = new ObjectType();

    private ObjectType() {

    }

    @Override
    public String getName() {
        return Types.TYPE_NAME_OBJECT;
    }

    @Override
    public Class<Object> getTypeClass() {
        return Object.class;
    }

    @Override
    public byte[] serialize(Object obj) {
        return SerializerFactory.getKryoSerializer().serialize(obj);
    }

    @Override
    public Object deserialize(byte[] bytes) {
        return SerializerFactory.getKryoSerializer().deserialize(bytes);
    }

    @Override
    public int compare(Object x, Object y) {
        if (x instanceof Comparable && y instanceof Comparable) {
            return ((Comparable) x).compareTo(y);
        }
        return 0;
    }

    @Override
    public boolean isPrimitive() {
        return true;
    }
}
