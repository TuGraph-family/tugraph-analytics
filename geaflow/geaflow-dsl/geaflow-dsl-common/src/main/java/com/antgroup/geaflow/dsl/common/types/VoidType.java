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

import com.antgroup.geaflow.common.type.IType;

public class VoidType implements IType<Void> {

    public static VoidType INSTANCE = new VoidType();

    private VoidType() {

    }

    @Override
    public String getName() {
        return "VOID";
    }

    @Override
    public Class<Void> getTypeClass() {
        return Void.class;
    }

    @Override
    public byte[] serialize(Void obj) {
        return new byte[0];
    }

    @Override
    public Void deserialize(byte[] bytes) {
        return null;
    }

    @Override
    public int compare(Void x, Void y) {
        return 0;
    }

    @Override
    public boolean isPrimitive() {
        return true;
    }
}
