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

public class ClassType implements IType<Class> {

    public static final ClassType INSTANCE = new ClassType();

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("{");
        sb.append("type:Class");
        sb.append("}");
        return sb.toString();
    }

    @Override
    public String getName() {
        return Types.TYPE_NAME_CLASS;
    }

    @Override
    public Class<Class> getTypeClass() {
        return Class.class;
    }

    @Override
    public byte[] serialize(Class obj) {
        return SerializerFactory.getKryoSerializer().serialize(obj);
    }

    @Override
    public Class deserialize(byte[] bytes) {
        return (Class) SerializerFactory.getKryoSerializer().deserialize(bytes);
    }

    @Override
    public int compare(Class x, Class y) {
        return x.toString().compareTo(y.toString());
    }

    @Override
    public boolean isPrimitive() {
        return false;
    }
}
